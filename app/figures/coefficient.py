"""Coefficient plot for displaying regression estimates with CIs.

Generates a clean coefficient plot (forest plot style) for one or more
regression specifications. Supports horizontal and vertical orientations,
multiple specifications side by side, and custom labels.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.figure import Figure
from scipy import stats as sp_stats

from .style import COLORS, MARKER_CYCLE


@dataclass
class EstimationResult:
    """Lightweight container for regression output.

    This duplicates the definition in tables.regression for independence.
    Both can be replaced by a shared types module when the project matures.
    """

    coefficients: dict[str, float]
    std_errors: dict[str, float]
    pvalues: dict[str, float]
    nobs: int = 0
    r_squared: float = 0.0
    fixed_effects: list[str] = field(default_factory=list)
    cluster_var: str | None = None
    dep_var: str = ""
    controls: list[str] = field(default_factory=list)


def coefficient_plot(
    results: EstimationResult | list[EstimationResult],
    var_names: list[str] | None = None,
    labels: dict[str, str] | None = None,
    spec_labels: list[str] | None = None,
    horizontal: bool = True,
    ci_level: float = 0.95,
    reference_line: float = 0,
    title: str | None = None,
    figsize: tuple[float, float] | None = None,
) -> Figure:
    """Create a coefficient plot with confidence intervals.

    Parameters
    ----------
    results : EstimationResult or list[EstimationResult]
        One or more estimation results. Multiple results are shown as
        offset points for each variable.
    var_names : list[str], optional
        Variables to include. If None, all variables from the first result
        are shown (excluding controls).
    labels : dict[str, str], optional
        Display labels for variables.
    spec_labels : list[str], optional
        Legend labels for each specification (when multiple results given).
    horizontal : bool
        If True (default), variables on the y-axis, coefficients on x-axis.
    ci_level : float
        Confidence interval level (default 0.95).
    reference_line : float
        Position of the reference (null) line (default 0).
    title : str, optional
        Figure title.
    figsize : tuple, optional
        Figure dimensions. Defaults to style settings.

    Returns
    -------
    matplotlib.figure.Figure
    """
    # Normalize to list
    if isinstance(results, EstimationResult):
        results = [results]

    if labels is None:
        labels = {}
    if spec_labels is None:
        spec_labels = [f"({i + 1})" for i in range(len(results))]

    # Determine variables to plot
    if var_names is None:
        var_names = [v for v in results[0].coefficients if v not in (results[0].controls or [])]

    n_vars = len(var_names)
    n_specs = len(results)
    z = sp_stats.norm.ppf(1 - (1 - ci_level) / 2)

    # Vertical positions for each variable
    positions = np.arange(n_vars)

    # Offset for multiple specifications
    if n_specs > 1:
        total_width = 0.6
        offsets = np.linspace(-total_width / 2, total_width / 2, n_specs)
    else:
        offsets = [0.0]

    fig, ax = plt.subplots(figsize=figsize)

    for spec_idx, (res, offset) in enumerate(zip(results, offsets, strict=False)):
        coefs = []
        ci_lower = []
        ci_upper = []
        valid_positions = []

        for var_idx, var in enumerate(var_names):
            if var in res.coefficients:
                b = res.coefficients[var]
                se = res.std_errors[var]
                coefs.append(b)
                ci_lower.append(b - z * se)
                ci_upper.append(b + z * se)
                valid_positions.append(positions[var_idx] + offset)

        coefs = np.array(coefs)
        ci_lower = np.array(ci_lower)
        ci_upper = np.array(ci_upper)
        valid_positions = np.array(valid_positions)

        color = list(COLORS.values())[spec_idx % len(COLORS)]
        marker = MARKER_CYCLE[spec_idx % len(MARKER_CYCLE)]

        if horizontal:
            # CI lines
            for i in range(len(coefs)):
                ax.plot(
                    [ci_lower[i], ci_upper[i]],
                    [valid_positions[i], valid_positions[i]],
                    color=color,
                    linewidth=1.5,
                    solid_capstyle="round",
                )
            # Point estimates
            ax.scatter(
                coefs,
                valid_positions,
                color=color,
                marker=marker,
                s=40,
                zorder=5,
                label=spec_labels[spec_idx] if n_specs > 1 else None,
            )
        else:
            for i in range(len(coefs)):
                ax.plot(
                    [valid_positions[i], valid_positions[i]],
                    [ci_lower[i], ci_upper[i]],
                    color=color,
                    linewidth=1.5,
                    solid_capstyle="round",
                )
            ax.scatter(
                valid_positions,
                coefs,
                color=color,
                marker=marker,
                s=40,
                zorder=5,
                label=spec_labels[spec_idx] if n_specs > 1 else None,
            )

    # Reference line
    display_labels = [labels.get(v, v) for v in var_names]

    if horizontal:
        ax.axvline(reference_line, color=COLORS["medium_gray"], linestyle="--", linewidth=0.8)
        ax.set_yticks(positions)
        ax.set_yticklabels(display_labels)
        ax.set_xlabel("Coefficient estimate")
        ax.invert_yaxis()
    else:
        ax.axhline(reference_line, color=COLORS["medium_gray"], linestyle="--", linewidth=0.8)
        ax.set_xticks(positions)
        ax.set_xticklabels(display_labels, rotation=45, ha="right")
        ax.set_ylabel("Coefficient estimate")

    if title:
        ax.set_title(title)

    if n_specs > 1:
        ax.legend()

    ci_pct = int(ci_level * 100)
    note_text = f"{ci_pct}% confidence intervals shown"
    ax.annotate(
        note_text,
        xy=(0.0, -0.08),
        xycoords="axes fraction",
        fontsize=7,
        color=COLORS["medium_gray"],
    )

    return fig
