"""Balance test table generator.

Produces a covariate balance table for randomized experiments or
regression discontinuity designs. Reports treatment and control means,
raw difference, standard error, p-value, and normalized difference.
Includes a joint F-test for overall balance at the bottom.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats as sp_stats


def _escape_latex(text: str) -> str:
    """Escape special LaTeX characters."""
    for char, repl in [
        ("&", r"\&"),
        ("%", r"\%"),
        ("$", r"\$"),
        ("#", r"\#"),
        ("_", r"\_"),
        ("~", r"\textasciitilde{}"),
    ]:
        text = text.replace(char, repl)
    return text


def _fmt(value: float, digits: int) -> str:
    return f"{value:.{digits}f}"


def _stars(pval: float) -> str:
    """Return significance stars for a p-value."""
    if pval < 0.01:
        return "^{***}"
    if pval < 0.05:
        return "^{**}"
    if pval < 0.10:
        return "^{*}"
    return ""


def _normalized_diff(treat_series: pd.Series, ctrl_series: pd.Series) -> float:
    """Compute the normalized difference (Imbens and Rubin, 2015).

    ndiff = (mean_T - mean_C) / sqrt((var_T + var_C) / 2)

    Values above 0.25 in absolute terms suggest meaningful imbalance.
    """
    var_t = treat_series.var()
    var_c = ctrl_series.var()
    denom = np.sqrt((var_t + var_c) / 2.0)
    if denom == 0:
        return 0.0
    return (treat_series.mean() - ctrl_series.mean()) / denom


def _joint_f_test(
    df: pd.DataFrame,
    variables: list[str],
    treat_col: str,
) -> tuple[float, float, int, int]:
    """Run joint F-test: regress treatment on all covariates.

    Returns (F-stat, p-value, df_num, df_denom).
    """
    from numpy.linalg import lstsq

    subset = df[variables + [treat_col]].dropna()
    y = subset[treat_col].values.astype(float)
    X = subset[variables].values.astype(float)
    n, k = X.shape

    # Add intercept
    X_full = np.column_stack([np.ones(n), X])

    # Restricted model: intercept only
    X_restricted = np.ones((n, 1))

    # Full model
    beta_full, _, _, _ = lstsq(X_full, y, rcond=None)
    resid_full = y - X_full @ beta_full
    ssr_full = resid_full @ resid_full

    # Restricted model
    beta_r, _, _, _ = lstsq(X_restricted, y, rcond=None)
    resid_r = y - X_restricted @ beta_r
    ssr_restricted = resid_r @ resid_r

    df_num = k
    df_denom = n - k - 1

    if df_denom <= 0 or ssr_full == 0:
        return np.nan, np.nan, df_num, max(df_denom, 1)

    f_stat = ((ssr_restricted - ssr_full) / df_num) / (ssr_full / df_denom)
    p_value = 1.0 - sp_stats.f.cdf(f_stat, df_num, df_denom)

    return f_stat, p_value, df_num, df_denom


def balance_table(
    df: pd.DataFrame,
    variables: list[str],
    treat_col: str,
    labels: dict[str, str] | None = None,
    digits: int = 3,
    title: str = "Covariate Balance",
) -> str:
    """Generate a covariate balance test table in LaTeX.

    Parameters
    ----------
    df : pd.DataFrame
        Source data containing covariates and the treatment indicator.
    variables : list[str]
        Covariate column names to test.
    treat_col : str
        Binary (0/1) treatment column.
    labels : dict[str, str], optional
        Display labels for covariates.
    digits : int
        Decimal places.
    title : str
        Table caption.

    Returns
    -------
    str
        LaTeX table string.
    """
    if labels is None:
        labels = {}

    # Split into treatment and control
    treat_df = df[df[treat_col] == 1]
    ctrl_df = df[df[treat_col] == 0]

    lines: list[str] = []
    lines.append(r"\begin{table}[htbp]")
    lines.append(r"\centering")
    lines.append(r"\begin{threeparttable}")
    lines.append(rf"\caption{{{_escape_latex(title)}}}")
    lines.append(r"\begin{tabular}{lcccccc}")
    lines.append(r"\toprule")
    lines.append(r" & Control & Treatment & Difference & SE & $p$-value & Norm.\ Diff \\")
    lines.append(r"\midrule")

    for var in variables:
        label = _escape_latex(labels.get(var, var))

        t_series = treat_df[var].dropna()
        c_series = ctrl_df[var].dropna()

        mean_t = t_series.mean()
        mean_c = c_series.mean()
        diff = mean_t - mean_c

        # Welch t-test
        t_stat, p_val = sp_stats.ttest_ind(t_series, c_series, equal_var=False)

        # Standard error of the difference
        se_diff = np.sqrt(t_series.var() / len(t_series) + c_series.var() / len(c_series))

        # Normalized difference
        ndiff = _normalized_diff(t_series, c_series)

        # Format with stars on the difference
        diff_str = _fmt(diff, digits) + _stars(p_val)

        lines.append(
            f"{label}"
            f" & {_fmt(mean_c, digits)}"
            f" & {_fmt(mean_t, digits)}"
            f" & ${diff_str}$"
            f" & {_fmt(se_diff, digits)}"
            f" & {_fmt(p_val, digits)}"
            f" & [{_fmt(ndiff, digits)}]"
            r" \\"
        )

    lines.append(r"\midrule")

    # Sample sizes
    n_ctrl = len(ctrl_df)
    n_treat = len(treat_df)
    lines.append(f"Observations & {n_ctrl:,} & {n_treat:,} & & & & \\\\")

    # Joint F-test
    f_stat, f_pval, df_num, df_denom = _joint_f_test(df, variables, treat_col)
    if not np.isnan(f_stat):
        lines.append(r"\midrule")
        lines.append(
            f"Joint $F$-test & \\multicolumn{{6}}{{c}}"
            f"{{$F({df_num},{df_denom}) = {_fmt(f_stat, 2)}$,"
            f" $p = {_fmt(f_pval, digits)}$}} \\\\"
        )

    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")

    # Notes
    lines.append(r"\begin{tablenotes}[flushleft]")
    lines.append(r"\small")
    lines.append(
        r"\item \textit{Note:} "
        r"Columns report means for control and treatment groups. "
        r"Difference is treatment minus control. "
        r"Standard errors from Welch's $t$-test (unequal variances). "
        r"Normalized difference computed as "
        r"$(\bar{X}_T - \bar{X}_C) / \sqrt{(s^2_T + s^2_C)/2}$; "
        r"values above $|0.25|$ suggest meaningful imbalance "
        r"(Imbens and Rubin, 2015). "
        r"Joint $F$-test from a regression of treatment assignment "
        r"on all covariates."
    )
    lines.append(r"\item $^{*}$\,p$<$0.10; $^{**}$\,p$<$0.05; $^{***}$\,p$<$0.01.")
    lines.append(r"\end{tablenotes}")
    lines.append(r"\end{threeparttable}")
    lines.append(r"\end{table}")

    return "\n".join(lines)
