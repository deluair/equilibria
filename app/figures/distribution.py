"""Distribution visualization tools for empirical economics.

Provides KDE density plots, histogram + density overlays, and a McCrary
density test visualization for regression discontinuity designs.
"""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.figure import Figure
from scipy import stats as sp_stats

from .style import COLORS


def _to_array(series: pd.Series | np.ndarray | list) -> np.ndarray:
    """Convert input to a clean float64 ndarray, dropping NaN."""
    arr = np.asarray(series, dtype=float)
    return arr[~np.isnan(arr)]


def density_plot(
    series: pd.Series | np.ndarray | list,
    label: str | None = None,
    bandwidth: str | float = "scott",
    title: str | None = None,
    xlabel: str | None = None,
    ylabel: str = "Density",
    color: str | None = None,
    figsize: tuple[float, float] | None = None,
    ax: plt.Axes | None = None,
) -> Figure:
    """Kernel density estimation plot.

    Parameters
    ----------
    series : array-like
        Data to plot.
    label : str, optional
        Legend label.
    bandwidth : str or float
        Bandwidth selection method ('scott', 'silverman') or a numeric value.
    title : str, optional
        Figure title.
    xlabel : str, optional
        X-axis label.
    ylabel : str
        Y-axis label.
    color : str, optional
        Line color.
    figsize : tuple, optional
        Figure dimensions.
    ax : matplotlib.axes.Axes, optional
        Existing axes to plot on. If None, a new figure is created.

    Returns
    -------
    matplotlib.figure.Figure
    """
    data = _to_array(series)
    if color is None:
        color = COLORS["blue"]

    created_fig = ax is None
    if created_fig:
        fig, ax = plt.subplots(figsize=figsize)
    else:
        fig = ax.get_figure()

    # Compute KDE
    if isinstance(bandwidth, str):
        kde = sp_stats.gaussian_kde(data, bw_method=bandwidth)
    else:
        kde = sp_stats.gaussian_kde(data, bw_method=bandwidth / data.std())

    x_grid = np.linspace(data.min() - 0.5 * data.std(), data.max() + 0.5 * data.std(), 500)
    density = kde(x_grid)

    ax.plot(x_grid, density, color=color, linewidth=1.5, label=label)
    ax.fill_between(x_grid, density, alpha=0.15, color=color, linewidth=0)

    ax.set_xlabel(xlabel if xlabel else "")
    ax.set_ylabel(ylabel)
    if title:
        ax.set_title(title)
    if label:
        ax.legend()

    return fig


def histogram_with_density(
    series: pd.Series | np.ndarray | list,
    bins: int = 30,
    label: str | None = None,
    title: str | None = None,
    xlabel: str | None = None,
    ylabel: str = "Density",
    color: str | None = None,
    kde_color: str | None = None,
    figsize: tuple[float, float] | None = None,
) -> Figure:
    """Histogram with KDE overlay.

    Parameters
    ----------
    series : array-like
        Data to plot.
    bins : int
        Number of histogram bins.
    label : str, optional
        Legend label for the histogram.
    title : str, optional
        Figure title.
    xlabel : str, optional
        X-axis label.
    ylabel : str
        Y-axis label.
    color : str, optional
        Histogram bar color.
    kde_color : str, optional
        KDE line color.
    figsize : tuple, optional
        Figure dimensions.

    Returns
    -------
    matplotlib.figure.Figure
    """
    data = _to_array(series)
    if color is None:
        color = COLORS["light_gray"]
    if kde_color is None:
        kde_color = COLORS["blue"]

    fig, ax = plt.subplots(figsize=figsize)

    # Histogram (density-normalized)
    ax.hist(
        data,
        bins=bins,
        density=True,
        color=color,
        edgecolor="white",
        linewidth=0.5,
        alpha=0.7,
        label=label,
    )

    # KDE overlay
    kde = sp_stats.gaussian_kde(data, bw_method="scott")
    x_grid = np.linspace(data.min() - 0.5 * data.std(), data.max() + 0.5 * data.std(), 500)
    ax.plot(x_grid, kde(x_grid), color=kde_color, linewidth=1.5, label="KDE")

    ax.set_xlabel(xlabel if xlabel else "")
    ax.set_ylabel(ylabel)
    if title:
        ax.set_title(title)
    ax.legend()

    return fig


def mccrary_plot(
    running_var: pd.Series | np.ndarray | list,
    cutoff: float = 0,
    bins: int | None = None,
    bandwidth: float | None = None,
    title: str | None = None,
    xlabel: str = "Running variable",
    ylabel: str = "Density",
    figsize: tuple[float, float] | None = None,
) -> Figure:
    """McCrary (2008) density test visualization.

    Plots local polynomial density estimates on each side of the cutoff
    to visually assess manipulation of the running variable in an RD design.

    This is a visualization tool, not a formal test implementation. For
    formal inference, use rddensity or the McCrary test from R/Stata.

    Parameters
    ----------
    running_var : array-like
        The running variable (forcing variable).
    cutoff : float
        RD cutoff value (default 0).
    bins : int, optional
        Number of histogram bins. If None, uses a data-driven default.
    bandwidth : float, optional
        Bandwidth for local density estimation on each side. If None,
        uses 2x the IQR-based rule.
    title : str, optional
        Figure title.
    xlabel : str
        X-axis label.
    ylabel : str
        Y-axis label.
    figsize : tuple, optional
        Figure dimensions.

    Returns
    -------
    matplotlib.figure.Figure
    """
    data = _to_array(running_var)

    if bins is None:
        # Use Freedman-Diaconis rule
        iqr = np.percentile(data, 75) - np.percentile(data, 25)
        if iqr == 0:
            bins = 30
        else:
            bin_width = 2 * iqr / (len(data) ** (1 / 3))
            bins = max(int((data.max() - data.min()) / bin_width), 10)

    if bandwidth is None:
        iqr = np.percentile(data, 75) - np.percentile(data, 25)
        bandwidth = 2 * max(iqr, data.std() * 0.5)

    # Split at the cutoff
    left = data[data < cutoff]
    right = data[data >= cutoff]

    fig, ax = plt.subplots(figsize=figsize)

    # Histogram for both sides
    all_edges = np.histogram_bin_edges(data, bins=bins)
    ax.hist(
        left,
        bins=all_edges,
        density=True,
        color=COLORS["blue"],
        alpha=0.3,
        edgecolor="white",
        linewidth=0.5,
    )
    ax.hist(
        right,
        bins=all_edges,
        density=True,
        color=COLORS["red"],
        alpha=0.3,
        edgecolor="white",
        linewidth=0.5,
    )

    # Local KDE on each side
    if len(left) > 5:
        left_window = left[left > (cutoff - bandwidth)]
        if len(left_window) > 2:
            kde_left = sp_stats.gaussian_kde(left_window, bw_method="scott")
            x_left = np.linspace(max(left.min(), cutoff - bandwidth), cutoff, 200)
            ax.plot(x_left, kde_left(x_left), color=COLORS["blue"], linewidth=1.5)

    if len(right) > 5:
        right_window = right[right < (cutoff + bandwidth)]
        if len(right_window) > 2:
            kde_right = sp_stats.gaussian_kde(right_window, bw_method="scott")
            x_right = np.linspace(cutoff, min(right.max(), cutoff + bandwidth), 200)
            ax.plot(x_right, kde_right(x_right), color=COLORS["red"], linewidth=1.5)

    # Cutoff line
    ax.axvline(
        cutoff,
        color=COLORS["dark_gray"],
        linewidth=1.2,
        linestyle="--",
        label=f"Cutoff = {cutoff}",
    )

    # Annotations: count on each side
    ax.annotate(
        f"N(left) = {len(left):,}",
        xy=(0.02, 0.95),
        xycoords="axes fraction",
        fontsize=8,
        color=COLORS["blue"],
        verticalalignment="top",
    )
    ax.annotate(
        f"N(right) = {len(right):,}",
        xy=(0.98, 0.95),
        xycoords="axes fraction",
        fontsize=8,
        color=COLORS["red"],
        verticalalignment="top",
        horizontalalignment="right",
    )

    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    if title:
        ax.set_title(title)
    else:
        ax.set_title("McCrary Density Test (Visual)")
    ax.legend(fontsize=8)

    return fig
