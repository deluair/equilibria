"""Event study plot for difference-in-differences and related designs.

Generates a publication-quality event study plot with coefficient estimates,
shaded confidence intervals, a vertical treatment onset line, and a marked
reference period.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.figure import Figure
from scipy import stats as sp_stats

from .style import COLORS


@dataclass
class EventStudyResult:
    """Container for event study regression output.

    Attributes:
        periods: Relative time periods (e.g. -5, -4, ..., 0, 1, ..., 5).
        coefficients: Point estimates for each period.
        std_errors: Standard errors for each period.
        pvalues: P-values for each period (optional, computed from SEs if absent).
        reference_period: The omitted/reference period (coefficient normalized to 0).
        nobs: Number of observations.
        dep_var: Dependent variable name.
    """

    periods: list[int]
    coefficients: list[float]
    std_errors: list[float]
    pvalues: list[float] = field(default_factory=list)
    reference_period: int = -1
    nobs: int = 0
    dep_var: str = ""


def event_study_plot(
    result: EventStudyResult,
    title: str | None = None,
    xlabel: str = "Periods relative to treatment",
    ylabel: str = "Coefficient estimate",
    reference_period: int | None = None,
    ci_level: float = 0.95,
    treatment_onset: int = 0,
    shade_pre: bool = False,
    figsize: tuple[float, float] | None = None,
    color: str | None = None,
) -> Figure:
    """Create an event study plot.

    Parameters
    ----------
    result : EventStudyResult
        Event study regression output.
    title : str, optional
        Figure title.
    xlabel : str
        X-axis label.
    ylabel : str
        Y-axis label.
    reference_period : int, optional
        The omitted reference period. If None, uses result.reference_period.
    ci_level : float
        Confidence interval level (default 0.95).
    treatment_onset : int
        Period at which treatment begins (default 0). A vertical dashed
        line is drawn here.
    shade_pre : bool
        If True, lightly shade the pre-treatment region.
    figsize : tuple, optional
        Figure dimensions.
    color : str, optional
        Line/marker color. Defaults to COLORS['blue'].

    Returns
    -------
    matplotlib.figure.Figure
    """
    if reference_period is None:
        reference_period = result.reference_period
    if color is None:
        color = COLORS["blue"]

    periods = np.array(result.periods)
    coefs = np.array(result.coefficients)
    ses = np.array(result.std_errors)

    z = sp_stats.norm.ppf(1 - (1 - ci_level) / 2)
    ci_upper = coefs + z * ses
    ci_lower = coefs - z * ses

    fig, ax = plt.subplots(figsize=figsize)

    # Shaded confidence interval
    ax.fill_between(
        periods,
        ci_lower,
        ci_upper,
        alpha=0.2,
        color=color,
        linewidth=0,
    )

    # Point estimates connected by line
    ax.plot(
        periods,
        coefs,
        color=color,
        linewidth=1.5,
        marker="o",
        markersize=4,
        zorder=4,
    )

    # Reference period: mark with a distinct symbol
    ref_mask = periods == reference_period
    if ref_mask.any():
        ax.scatter(
            periods[ref_mask],
            coefs[ref_mask],
            color=COLORS["red"],
            marker="D",
            s=50,
            zorder=5,
            label=f"Reference (t = {reference_period})",
        )

    # Zero line
    ax.axhline(0, color=COLORS["dark_gray"], linewidth=0.8, linestyle="-")

    # Treatment onset vertical line
    ax.axvline(
        treatment_onset - 0.5,
        color=COLORS["dark_gray"],
        linewidth=1.0,
        linestyle="--",
        alpha=0.8,
    )

    # Optional shading of the pre-treatment region
    if shade_pre:
        ax.axvspan(
            periods.min() - 0.5,
            treatment_onset - 0.5,
            alpha=0.05,
            color=COLORS["medium_gray"],
        )

    # Treatment onset label
    y_range = ci_upper.max() - ci_lower.min()
    ax.annotate(
        "Treatment",
        xy=(treatment_onset - 0.5, ci_upper.max() + 0.05 * y_range),
        fontsize=8,
        ha="center",
        color=COLORS["dark_gray"],
    )

    # Axis labels and title
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    if title:
        ax.set_title(title)

    # Integer ticks for periods
    ax.set_xticks(periods)
    ax.set_xticklabels([str(int(p)) for p in periods])

    # Legend (only if reference period is shown)
    if ref_mask.any():
        ax.legend(loc="best", fontsize=8)

    # Confidence interval note
    ci_pct = int(ci_level * 100)
    ax.annotate(
        f"Shaded area: {ci_pct}% CI",
        xy=(0.0, -0.10),
        xycoords="axes fraction",
        fontsize=7,
        color=COLORS["medium_gray"],
    )

    return fig
