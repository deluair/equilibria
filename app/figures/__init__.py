"""Publication-quality figure generators for empirical economics.

All plots follow AER/QJE visual conventions: serif fonts, grayscale-safe
palettes, left+bottom spines only, 6.5 x 4.5 inch default size.
"""

from .binscatter import binscatter
from .coefficient import coefficient_plot
from .distribution import density_plot, histogram_with_density, mccrary_plot
from .event_study import event_study_plot
from .style import COLORS, save_figure, set_journal_style

__all__ = [
    "set_journal_style",
    "save_figure",
    "COLORS",
    "coefficient_plot",
    "event_study_plot",
    "binscatter",
    "density_plot",
    "histogram_with_density",
    "mccrary_plot",
]
