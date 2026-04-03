"""Regression table generator producing AER/QJE-style LaTeX output.

Generates multi-column regression tables with coefficients, standard errors
in parentheses, significance stars, and a bottom panel of summary statistics
(N, R-squared, fixed effects indicators, clustering level).

Uses booktabs + threeparttable for clean typesetting.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class EstimationResult:
    """Container for regression output from any estimator.

    Attributes:
        coefficients: Mapping of variable name to point estimate.
        std_errors: Mapping of variable name to standard error.
        pvalues: Mapping of variable name to p-value.
        nobs: Number of observations.
        r_squared: R-squared (or within-R-squared for FE models).
        fixed_effects: List of fixed-effect dimensions included (e.g. ["firm", "year"]).
        cluster_var: Variable(s) used for clustering, or None.
        dep_var: Dependent variable name.
        controls: List of control variable names included but not displayed.
    """

    coefficients: dict[str, float]
    std_errors: dict[str, float]
    pvalues: dict[str, float]
    nobs: int
    r_squared: float
    fixed_effects: list[str] = field(default_factory=list)
    cluster_var: str | None = None
    dep_var: str = ""
    controls: list[str] = field(default_factory=list)


def _stars(pval: float) -> str:
    """Return significance stars for a p-value."""
    if pval < 0.01:
        return "^{***}"
    if pval < 0.05:
        return "^{**}"
    if pval < 0.10:
        return "^{*}"
    return ""


def _fmt(value: float, digits: int) -> str:
    """Format a number to the specified decimal places."""
    return f"{value:.{digits}f}"


def _escape_latex(text: str) -> str:
    """Escape characters that are special in LaTeX."""
    replacements = {
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "~": r"\textasciitilde{}",
    }
    for char, escaped in replacements.items():
        text = text.replace(char, escaped)
    return text


def regression_table(
    results: list[EstimationResult],
    dep_var_label: str,
    col_labels: list[str] | None = None,
    covariate_labels: dict[str, str] | None = None,
    show_controls: list[str] | None = None,
    stars: bool = True,
    digits: int = 3,
    note: str | None = None,
) -> str:
    """Generate a publication-quality regression table in LaTeX.

    Parameters
    ----------
    results : list[EstimationResult]
        One EstimationResult per column.
    dep_var_label : str
        Label for the dependent variable, displayed in the table header.
    col_labels : list[str], optional
        Column headers (e.g. ["OLS", "IV", "FE"]). Defaults to (1), (2), ...
    covariate_labels : dict[str, str], optional
        Mapping from variable name to display label. Variables not in this
        dict are displayed with their raw name (underscores escaped).
    show_controls : list[str], optional
        Control variables to show explicitly. If None, only non-control
        covariates are displayed.
    stars : bool
        Whether to append significance stars.
    digits : int
        Decimal places for coefficients and standard errors.
    note : str, optional
        Additional text appended to the table note.

    Returns
    -------
    str
        A complete LaTeX table string (threeparttable + tabular + booktabs).
    """
    ncols = len(results)
    if col_labels is None:
        col_labels = [f"({i + 1})" for i in range(ncols)]
    if covariate_labels is None:
        covariate_labels = {}

    # Collect all covariates to display, preserving insertion order from
    # the first result that contains each variable.
    all_vars: list[str] = []
    for res in results:
        for var in res.coefficients:
            if var not in all_vars:
                if show_controls and var in show_controls:
                    all_vars.append(var)
                elif var not in (res.controls or []):
                    all_vars.append(var)

    # If show_controls is provided, also ensure those appear even if
    # they were in the controls list.
    if show_controls:
        for var in show_controls:
            if var not in all_vars:
                all_vars.append(var)

    col_spec = "l" + "c" * ncols
    lines: list[str] = []

    # Preamble
    lines.append(r"\begin{table}[htbp]")
    lines.append(r"\centering")
    lines.append(r"\begin{threeparttable}")
    lines.append(rf"\caption{{{_escape_latex(dep_var_label)}}}")
    lines.append(rf"\begin{{tabular}}{{{col_spec}}}")
    lines.append(r"\toprule")

    # Column headers
    header_cells = " & ".join(col_labels)
    lines.append(f" & {header_cells} \\\\")
    lines.append(r"\midrule")

    # Coefficient rows
    for var in all_vars:
        label = covariate_labels.get(var, _escape_latex(var))
        coef_cells: list[str] = []
        se_cells: list[str] = []

        for res in results:
            if var in res.coefficients:
                coef_str = _fmt(res.coefficients[var], digits)
                if stars:
                    coef_str += _stars(res.pvalues.get(var, 1.0))
                coef_str = f"${coef_str}$"
                se_str = f"$({_fmt(res.std_errors[var], digits)})$"
            else:
                coef_str = ""
                se_str = ""
            coef_cells.append(coef_str)
            se_cells.append(se_str)

        lines.append(f"{label} & {' & '.join(coef_cells)} \\\\")
        lines.append(f" & {' & '.join(se_cells)} \\\\[4pt]")

    # Bottom panel
    lines.append(r"\midrule")

    # Observations
    n_cells = [f"{res.nobs:,}" for res in results]
    lines.append(f"Observations & {' & '.join(n_cells)} \\\\")

    # R-squared
    r2_cells = [_fmt(res.r_squared, digits) for res in results]
    lines.append(f"$R^2$ & {' & '.join(r2_cells)} \\\\")

    # Fixed effects rows: collect all FE dimensions across columns
    all_fe: list[str] = []
    for res in results:
        for fe in res.fixed_effects:
            if fe not in all_fe:
                all_fe.append(fe)

    for fe in all_fe:
        fe_label = _escape_latex(fe.replace("_", " ").title()) + " FE"
        fe_cells = ["Yes" if fe in res.fixed_effects else "No" for res in results]
        lines.append(f"{fe_label} & {' & '.join(fe_cells)} \\\\")

    # Clustering
    cluster_vars_present = any(res.cluster_var for res in results)
    if cluster_vars_present:
        cl_cells = [
            _escape_latex(res.cluster_var) if res.cluster_var else "None" for res in results
        ]
        lines.append(f"Clustering & {' & '.join(cl_cells)} \\\\")

    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")

    # Table notes
    star_note = r"$^{*}$\,p$<$0.10; $^{**}$\,p$<$0.05; $^{***}$\,p$<$0.01."
    full_note = f"Standard errors in parentheses. {star_note}"
    if note:
        full_note += f" {note}"
    lines.append(r"\begin{tablenotes}[flushleft]")
    lines.append(r"\small")
    lines.append(rf"\item \textit{{Note:}} {full_note}")
    lines.append(r"\end{tablenotes}")

    lines.append(r"\end{threeparttable}")
    lines.append(r"\end{table}")

    return "\n".join(lines)
