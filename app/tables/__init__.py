"""Publication-quality table generators for empirical economics.

Outputs LaTeX strings formatted for AER/QJE submission standards.
Uses booktabs and threeparttable conventions throughout.
"""

from .balance import balance_table
from .regression import regression_table
from .summary_stats import summary_stats_table

__all__ = [
    "regression_table",
    "summary_stats_table",
    "balance_table",
]
