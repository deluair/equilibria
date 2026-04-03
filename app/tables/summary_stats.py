"""Summary statistics table generator.

Produces a LaTeX table of descriptive statistics (mean, SD, min, percentiles,
max, N) for a list of variables. Optionally splits by a grouping column and
reports a difference-in-means t-test.
"""

from __future__ import annotations

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


# Mapping from stat keyword to (display label, compute function).
_STAT_FUNCS: dict[str, tuple[str, callable]] = {
    "mean": ("Mean", lambda s: s.mean()),
    "sd": ("SD", lambda s: s.std()),
    "min": ("Min", lambda s: s.min()),
    "p10": ("P10", lambda s: s.quantile(0.10)),
    "p25": ("P25", lambda s: s.quantile(0.25)),
    "median": ("Median", lambda s: s.median()),
    "p75": ("P75", lambda s: s.quantile(0.75)),
    "p90": ("P90", lambda s: s.quantile(0.90)),
    "max": ("Max", lambda s: s.max()),
    "n": ("N", lambda s: int(s.count())),
}


def _compute_stats(
    series: pd.Series,
    stat_keys: list[str],
    digits: int,
) -> list[str]:
    """Compute requested statistics for a single series."""
    cells: list[str] = []
    for key in stat_keys:
        _, func = _STAT_FUNCS[key]
        val = func(series)
        if key == "n":
            cells.append(f"{int(val):,}")
        else:
            cells.append(_fmt(val, digits))
    return cells


def _diff_in_means(
    series_treat: pd.Series,
    series_ctrl: pd.Series,
    digits: int,
) -> tuple[str, str, str]:
    """Two-sample t-test for difference in means.

    Returns formatted (difference, t-stat, p-value) strings.
    """
    treat_clean = series_treat.dropna()
    ctrl_clean = series_ctrl.dropna()
    diff = treat_clean.mean() - ctrl_clean.mean()
    t_stat, p_val = sp_stats.ttest_ind(treat_clean, ctrl_clean, equal_var=False)
    return _fmt(diff, digits), _fmt(t_stat, digits), _fmt(p_val, digits)


def summary_stats_table(
    df: pd.DataFrame,
    variables: list[str],
    labels: dict[str, str] | None = None,
    stats: list[str] | None = None,
    by: str | None = None,
    digits: int = 3,
    title: str = "Summary Statistics",
) -> str:
    """Generate a summary statistics table in LaTeX.

    Parameters
    ----------
    df : pd.DataFrame
        Source data.
    variables : list[str]
        Column names to summarize.
    labels : dict[str, str], optional
        Display labels for each variable.
    stats : list[str], optional
        Statistics to compute. Defaults to
        ['mean', 'sd', 'min', 'p25', 'median', 'p75', 'max', 'n'].
    by : str, optional
        Column to split by (e.g. a treatment indicator). When provided,
        separate panels are shown and a difference-in-means test is appended.
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
    if stats is None:
        stats = ["mean", "sd", "min", "p25", "median", "p75", "max", "n"]

    for key in stats:
        if key not in _STAT_FUNCS:
            raise ValueError(f"Unknown stat '{key}'. Choose from: {list(_STAT_FUNCS)}")

    stat_headers = [_STAT_FUNCS[k][0] for k in stats]

    lines: list[str] = []
    lines.append(r"\begin{table}[htbp]")
    lines.append(r"\centering")
    lines.append(r"\begin{threeparttable}")
    lines.append(rf"\caption{{{_escape_latex(title)}}}")

    if by is None:
        # Single-panel table
        ncols = len(stats)
        col_spec = "l" + "c" * ncols
        lines.append(rf"\begin{{tabular}}{{{col_spec}}}")
        lines.append(r"\toprule")
        header = " & ".join(stat_headers)
        lines.append(f"Variable & {header} \\\\")
        lines.append(r"\midrule")

        for var in variables:
            label = _escape_latex(labels.get(var, var))
            cells = _compute_stats(df[var], stats, digits)
            lines.append(f"{label} & {' & '.join(cells)} \\\\")

        lines.append(r"\bottomrule")
        lines.append(r"\end{tabular}")
    else:
        # Two-panel table with difference-in-means
        groups = sorted(df[by].dropna().unique())
        if len(groups) != 2:
            raise ValueError(
                f"Expected exactly 2 groups in column '{by}', got {len(groups)}: {groups}"
            )
        g0, g1 = groups
        df0 = df[df[by] == g0]
        df1 = df[df[by] == g1]

        # Columns: stat headers for each group + Diff + t-stat + p-value
        n_stat_cols = len(stats)
        ncols = n_stat_cols * 2 + 3
        col_spec = "l" + "c" * ncols
        lines.append(rf"\begin{{tabular}}{{{col_spec}}}")
        lines.append(r"\toprule")

        # Group header row with multicolumn spans
        group_label_0 = _escape_latex(str(g0))
        group_label_1 = _escape_latex(str(g1))
        lines.append(
            f" & \\multicolumn{{{n_stat_cols}}}{{c}}{{{group_label_0}}}"
            f" & \\multicolumn{{{n_stat_cols}}}{{c}}{{{group_label_1}}}"
            r" & Diff & $t$ & $p$ \\"
        )
        lines.append(
            rf"\cmidrule(lr){{2-{1 + n_stat_cols}}}"
            rf" \cmidrule(lr){{{2 + n_stat_cols}-{1 + 2 * n_stat_cols}}}"
        )

        # Stat labels row
        stat_row = " & ".join(stat_headers) + " & " + " & ".join(stat_headers)
        lines.append(f" & {stat_row} & & & \\\\")
        lines.append(r"\midrule")

        for var in variables:
            label = _escape_latex(labels.get(var, var))
            cells0 = _compute_stats(df0[var], stats, digits)
            cells1 = _compute_stats(df1[var], stats, digits)
            diff_str, t_str, p_str = _diff_in_means(df1[var], df0[var], digits)
            all_cells = cells0 + cells1 + [diff_str, t_str, p_str]
            lines.append(f"{label} & {' & '.join(all_cells)} \\\\")

        n0 = len(df0)
        n1 = len(df1)
        lines.append(r"\midrule")
        lines.append(
            f"Observations & \\multicolumn{{{n_stat_cols}}}{{c}}{{{n0:,}}}"
            f" & \\multicolumn{{{n_stat_cols}}}{{c}}{{{n1:,}}}"
            r" & & & \\"
        )

        lines.append(r"\bottomrule")
        lines.append(r"\end{tabular}")

    # Notes
    lines.append(r"\begin{tablenotes}[flushleft]")
    lines.append(r"\small")
    if by is not None:
        lines.append(
            r"\item \textit{Note:} Difference is computed as "
            f"({_escape_latex(str(g1))}) $-$ ({_escape_latex(str(g0))}). "
            r"$t$-statistics from Welch's two-sample $t$-test (unequal variances)."
        )
    else:
        lines.append(
            r"\item \textit{Note:} SD denotes standard deviation. "
            r"P25 and P75 denote the 25th and 75th percentiles."
        )
    lines.append(r"\end{tablenotes}")
    lines.append(r"\end{threeparttable}")
    lines.append(r"\end{table}")

    return "\n".join(lines)
