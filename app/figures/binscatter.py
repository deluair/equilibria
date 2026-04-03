"""Binned scatter plot (binscatter) for visualizing conditional means.

Implements the standard econometric binscatter: residualize y and x against
controls and/or fixed effects, bin the residualized x into equal-sized bins,
compute the mean of residualized y within each bin, and plot the result.
Optionally overlays a linear fit with confidence band.
"""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.figure import Figure
from scipy import stats as sp_stats

from .style import COLORS


def _residualize(
    df: pd.DataFrame,
    target: str,
    controls: list[str] | None = None,
    absorb: list[str] | None = None,
) -> np.ndarray:
    """Residualize target variable against controls and/or fixed effects.

    Uses OLS partialling-out. Fixed effects are demeaned iteratively
    (simple within-transformation).

    Parameters
    ----------
    df : pd.DataFrame
        Source data.
    target : str
        Column to residualize.
    controls : list[str], optional
        Continuous control variables.
    absorb : list[str], optional
        Categorical variables to absorb as fixed effects.

    Returns
    -------
    np.ndarray
        Residualized values, same length as df.
    """
    y = df[target].values.astype(float).copy()
    mask = ~np.isnan(y)

    # Absorb fixed effects by iterative demeaning
    if absorb:
        for fe_col in absorb:
            groups = df[fe_col].values
            unique_groups = np.unique(groups[mask])
            group_means = {}
            for g in unique_groups:
                g_mask = (groups == g) & mask
                group_means[g] = np.nanmean(y[g_mask])
            for g in unique_groups:
                g_mask = (groups == g) & mask
                y[g_mask] -= group_means[g]

    # Partial out continuous controls via OLS
    if controls:
        X = df[controls].values.astype(float)
        valid = mask & ~np.any(np.isnan(X), axis=1)
        X_valid = np.column_stack([np.ones(valid.sum()), X[valid]])
        y_valid = y[valid]
        beta, _, _, _ = np.linalg.lstsq(X_valid, y_valid, rcond=None)
        # Compute residuals for all valid observations
        X_all = np.column_stack([np.ones(mask.sum()), X[mask]])
        y[mask] -= X_all @ beta

    return y


def binscatter(
    df: pd.DataFrame,
    y: str,
    x: str,
    n_bins: int = 20,
    controls: list[str] | None = None,
    absorb: list[str] | None = None,
    fit_line: bool = True,
    ci: bool = True,
    ci_level: float = 0.95,
    title: str | None = None,
    xlabel: str | None = None,
    ylabel: str | None = None,
    figsize: tuple[float, float] | None = None,
    color: str | None = None,
) -> Figure:
    """Create a binned scatter plot.

    Parameters
    ----------
    df : pd.DataFrame
        Source data.
    y : str
        Outcome variable column name.
    x : str
        Running/explanatory variable column name.
    n_bins : int
        Number of equal-frequency bins (default 20).
    controls : list[str], optional
        Continuous controls to partial out before binning.
    absorb : list[str], optional
        Categorical FE variables to absorb before binning.
    fit_line : bool
        Overlay a linear fit through the binned means.
    ci : bool
        Show a confidence band around the fit line.
    ci_level : float
        Confidence level for the band (default 0.95).
    title : str, optional
        Figure title.
    xlabel : str, optional
        X-axis label. Defaults to the column name.
    ylabel : str, optional
        Y-axis label. Defaults to the column name.
    figsize : tuple, optional
        Figure dimensions.
    color : str, optional
        Color for scatter points and fit line.

    Returns
    -------
    matplotlib.figure.Figure
    """
    if color is None:
        color = COLORS["blue"]

    # Work on a clean copy
    cols_needed = [y, x]
    if controls:
        cols_needed += controls
    if absorb:
        cols_needed += absorb
    work = df[cols_needed].dropna().copy()

    # Residualize if controls or FE specified
    has_adjustments = bool(controls) or bool(absorb)
    if has_adjustments:
        y_resid = _residualize(work, y, controls=controls, absorb=absorb)
        x_resid = _residualize(work, x, controls=controls, absorb=absorb)
    else:
        y_resid = work[y].values.astype(float)
        x_resid = work[x].values.astype(float)

    # Create equal-frequency bins based on x
    bin_edges = np.percentile(x_resid, np.linspace(0, 100, n_bins + 1))
    bin_indices = np.digitize(x_resid, bin_edges, right=True)
    bin_indices = np.clip(bin_indices, 1, n_bins)

    bin_x_means = np.zeros(n_bins)
    bin_y_means = np.zeros(n_bins)
    bin_counts = np.zeros(n_bins)

    for b in range(1, n_bins + 1):
        mask = bin_indices == b
        if mask.sum() > 0:
            bin_x_means[b - 1] = x_resid[mask].mean()
            bin_y_means[b - 1] = y_resid[mask].mean()
            bin_counts[b - 1] = mask.sum()

    # Remove empty bins
    valid = bin_counts > 0
    bin_x_means = bin_x_means[valid]
    bin_y_means = bin_y_means[valid]

    fig, ax = plt.subplots(figsize=figsize)

    # Scatter the bin means
    ax.scatter(
        bin_x_means,
        bin_y_means,
        color=color,
        s=35,
        zorder=5,
        edgecolors="white",
        linewidth=0.5,
    )

    # Fit line through individual data (not bin means) for correct inference
    slope, intercept, r_value, p_value, std_err = sp_stats.linregress(x_resid, y_resid)
    r_squared = r_value**2

    if fit_line:
        x_line = np.linspace(bin_x_means.min(), bin_x_means.max(), 200)
        y_line = intercept + slope * x_line
        ax.plot(x_line, y_line, color=COLORS["red"], linewidth=1.2, zorder=4)

        if ci:
            n = len(x_resid)
            x_mean = x_resid.mean()
            ss_x = np.sum((x_resid - x_mean) ** 2)
            resid = y_resid - (intercept + slope * x_resid)
            mse = np.sum(resid**2) / (n - 2)

            t_crit = sp_stats.t.ppf(1 - (1 - ci_level) / 2, n - 2)
            se_line = np.sqrt(mse * (1.0 / n + (x_line - x_mean) ** 2 / ss_x))
            ci_upper = y_line + t_crit * se_line
            ci_lower = y_line - t_crit * se_line

            ax.fill_between(
                x_line,
                ci_lower,
                ci_upper,
                alpha=0.15,
                color=COLORS["red"],
                linewidth=0,
            )

    # Annotation: slope and R-squared
    annotation = f"Slope = {slope:.4f} (SE = {std_err:.4f})\n$R^2$ = {r_squared:.4f}"
    if has_adjustments:
        annotation += "\n(residualized)"
    ax.annotate(
        annotation,
        xy=(0.03, 0.97),
        xycoords="axes fraction",
        fontsize=8,
        verticalalignment="top",
        bbox=dict(boxstyle="round,pad=0.3", fc="white", ec=COLORS["light_gray"], alpha=0.8),
    )

    # Labels
    ax.set_xlabel(xlabel if xlabel else x)
    ax.set_ylabel(ylabel if ylabel else y)
    if title:
        ax.set_title(title)

    return fig
