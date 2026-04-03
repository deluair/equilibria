"""Journal-ready matplotlib style configuration.

Provides a single function to configure matplotlib rcParams for
publication-quality output matching AER, QJE, or Econometrica conventions.
Also provides a grayscale-safe color palette and a helper to save figures
in multiple formats.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.figure import Figure

# Grayscale-safe palette. Each color is distinguishable in both color
# and grayscale print. Ordered by decreasing contrast against white.
COLORS: dict[str, str] = {
    "black": "#000000",
    "dark_gray": "#404040",
    "medium_gray": "#808080",
    "light_gray": "#BFBFBF",
    "blue": "#1F77B4",
    "red": "#D62728",
    "green": "#2CA02C",
    "orange": "#FF7F0E",
    "purple": "#9467BD",
    "teal": "#17BECF",
}

# Ordered list for cycling through in multi-series plots.
COLOR_CYCLE: list[str] = [
    COLORS["black"],
    COLORS["blue"],
    COLORS["red"],
    COLORS["green"],
    COLORS["orange"],
    COLORS["purple"],
    COLORS["teal"],
    COLORS["dark_gray"],
]

# Marker cycle for distinguishing series in grayscale.
MARKER_CYCLE: list[str] = ["o", "s", "^", "D", "v", "P", "X", "*"]

# Journal-specific defaults.
_JOURNAL_DEFAULTS: dict[str, dict] = {
    "aer": {
        "figure.figsize": (6.5, 4.5),
        "font.size": 10,
        "font.family": "serif",
        "font.serif": ["Times New Roman", "Times", "Computer Modern Roman"],
    },
    "qje": {
        "figure.figsize": (6.5, 4.5),
        "font.size": 10,
        "font.family": "serif",
        "font.serif": ["Times New Roman", "Times", "Computer Modern Roman"],
    },
    "econometrica": {
        "figure.figsize": (5.5, 4.0),
        "font.size": 9,
        "font.family": "serif",
        "font.serif": ["Computer Modern Roman", "Times New Roman", "Times"],
    },
    "restud": {
        "figure.figsize": (6.0, 4.5),
        "font.size": 10,
        "font.family": "serif",
        "font.serif": ["Times New Roman", "Times", "Computer Modern Roman"],
    },
}


def set_journal_style(journal: str = "aer") -> None:
    """Configure matplotlib rcParams for journal-quality figures.

    Parameters
    ----------
    journal : str
        Target journal style. One of 'aer', 'qje', 'econometrica', 'restud'.
        Defaults to 'aer'.
    """
    journal = journal.lower()
    if journal not in _JOURNAL_DEFAULTS:
        raise ValueError(f"Unknown journal '{journal}'. Choose from: {list(_JOURNAL_DEFAULTS)}")

    journal_cfg = _JOURNAL_DEFAULTS[journal]

    # Reset to defaults first to avoid state leakage
    matplotlib.rcdefaults()

    # Font settings
    plt.rcParams["font.family"] = journal_cfg["font.family"]
    plt.rcParams["font.serif"] = journal_cfg["font.serif"]
    plt.rcParams["font.size"] = journal_cfg["font.size"]

    # Use Type 1 / TrueType fonts for PDF (required by many journals)
    plt.rcParams["pdf.fonttype"] = 42
    plt.rcParams["ps.fonttype"] = 42

    # Figure size
    plt.rcParams["figure.figsize"] = journal_cfg["figure.figsize"]
    plt.rcParams["figure.dpi"] = 150
    plt.rcParams["savefig.dpi"] = 300
    plt.rcParams["savefig.bbox"] = "tight"
    plt.rcParams["savefig.pad_inches"] = 0.05

    # Axes: left and bottom spines only
    plt.rcParams["axes.spines.top"] = False
    plt.rcParams["axes.spines.right"] = False
    plt.rcParams["axes.linewidth"] = 0.8
    plt.rcParams["axes.labelsize"] = journal_cfg["font.size"]
    plt.rcParams["axes.titlesize"] = journal_cfg["font.size"] + 1
    plt.rcParams["axes.prop_cycle"] = plt.cycler(color=COLOR_CYCLE)

    # Grid: light, dashed
    plt.rcParams["axes.grid"] = True
    plt.rcParams["grid.color"] = "#D0D0D0"
    plt.rcParams["grid.linestyle"] = "--"
    plt.rcParams["grid.linewidth"] = 0.5
    plt.rcParams["grid.alpha"] = 0.7

    # Ticks
    plt.rcParams["xtick.labelsize"] = journal_cfg["font.size"] - 1
    plt.rcParams["ytick.labelsize"] = journal_cfg["font.size"] - 1
    plt.rcParams["xtick.direction"] = "out"
    plt.rcParams["ytick.direction"] = "out"
    plt.rcParams["xtick.major.size"] = 4
    plt.rcParams["ytick.major.size"] = 4
    plt.rcParams["xtick.minor.size"] = 2
    plt.rcParams["ytick.minor.size"] = 2

    # Legend
    plt.rcParams["legend.fontsize"] = journal_cfg["font.size"] - 1
    plt.rcParams["legend.frameon"] = False
    plt.rcParams["legend.loc"] = "best"

    # Lines
    plt.rcParams["lines.linewidth"] = 1.5
    plt.rcParams["lines.markersize"] = 5

    # Layout
    plt.rcParams["figure.constrained_layout.use"] = True


def save_figure(
    fig: Figure,
    path: str | Path,
    formats: list[str] | None = None,
    dpi: int = 300,
    transparent: bool = False,
) -> list[Path]:
    """Save a matplotlib figure in one or more formats.

    Parameters
    ----------
    fig : matplotlib.figure.Figure
        The figure to save.
    path : str or Path
        Base output path (without extension). If an extension is present,
        it is stripped and the file is saved with each requested format.
    formats : list[str], optional
        Output formats. Defaults to ['pdf', 'png'].
    dpi : int
        Resolution for raster formats.
    transparent : bool
        Whether to save with a transparent background.

    Returns
    -------
    list[Path]
        Paths to all saved files.
    """
    if formats is None:
        formats = ["pdf", "png"]

    base = Path(path)
    if base.suffix and base.suffix.lstrip(".") in ("pdf", "png", "svg", "eps", "tiff"):
        base = base.with_suffix("")

    base.parent.mkdir(parents=True, exist_ok=True)

    saved: list[Path] = []
    for fmt in formats:
        out = base.with_suffix(f".{fmt}")
        fig.savefig(
            out,
            format=fmt,
            dpi=dpi,
            bbox_inches="tight",
            transparent=transparent,
        )
        saved.append(out)

    return saved
