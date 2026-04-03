"""Standardized result classes for all estimation methods."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np


@dataclass
class EstimationResult:
    """Standardized output from any estimation method.

    Attributes:
        coef: Dictionary mapping variable names to coefficient estimates.
        se: Standard errors, same keys as coef.
        pval: P-values, same keys as coef.
        ci_lower: Lower bound of 95% confidence interval.
        ci_upper: Upper bound of 95% confidence interval.
        n_obs: Number of observations used.
        r_sq: R-squared (or pseudo R-squared where applicable).
        adj_r_sq: Adjusted R-squared (None if not applicable).
        method: Name of the estimation method (e.g. "OLS", "IV-2SLS").
        depvar: Name of the dependent variable.
        diagnostics: Method-specific diagnostics (F-stat, J-test, etc.).
    """

    coef: dict[str, float]
    se: dict[str, float]
    pval: dict[str, float]
    ci_lower: dict[str, float]
    ci_upper: dict[str, float]
    n_obs: int
    r_sq: float
    adj_r_sq: float | None = None
    method: str = "OLS"
    depvar: str = ""
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        lines = [
            f"=== {self.method} Results ===",
            f"Dep. variable: {self.depvar}",
            f"N = {self.n_obs:,}    R² = {self.r_sq:.4f}"
            + (f"    Adj. R² = {self.adj_r_sq:.4f}" if self.adj_r_sq is not None else ""),
            "",
            f"{'Variable':<24} {'Coef':>10} {'Std.Err':>10} {'P>|t|':>8} {'[95% CI]':>22}",
            "-" * 78,
        ]
        for var in self.coef:
            stars = _significance_stars(self.pval[var])
            ci_str = f"[{self.ci_lower[var]:>9.4f}, {self.ci_upper[var]:>9.4f}]"
            lines.append(
                f"{var:<24} {self.coef[var]:>10.4f} {self.se[var]:>10.4f} "
                f"{self.pval[var]:>8.4f}{stars}  {ci_str}"
            )
        lines.append("-" * 78)
        if self.diagnostics:
            lines.append("Diagnostics:")
            for k, v in self.diagnostics.items():
                lines.append(f"  {k}: {v}")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dictionary."""
        return {
            "method": self.method,
            "depvar": self.depvar,
            "n_obs": self.n_obs,
            "r_sq": self.r_sq,
            "adj_r_sq": self.adj_r_sq,
            "coefficients": {
                var: {
                    "coef": self.coef[var],
                    "se": self.se[var],
                    "pval": self.pval[var],
                    "ci_lower": self.ci_lower[var],
                    "ci_upper": self.ci_upper[var],
                }
                for var in self.coef
            },
            "diagnostics": self.diagnostics,
        }

    def significant_at(self, alpha: float = 0.05) -> list[str]:
        """Return variable names significant at the given alpha level."""
        return [var for var, p in self.pval.items() if p < alpha]


@dataclass
class EventStudyResult:
    """Output from an event study / dynamic DID estimation.

    Attributes:
        periods: List of relative time periods.
        coef: Coefficient for each period.
        se: Standard error for each period.
        pval: P-value for each period.
        ci_lower: Lower 95% CI for each period.
        ci_upper: Upper 95% CI for each period.
        ref_period: The omitted reference period.
        pre_trend_fstat: Joint F-statistic for pre-treatment coefficients.
        pre_trend_pval: P-value of the pre-trend test.
        n_obs: Number of observations.
        depvar: Dependent variable name.
        diagnostics: Additional diagnostics.
    """

    periods: list[int]
    coef: list[float]
    se: list[float]
    pval: list[float]
    ci_lower: list[float]
    ci_upper: list[float]
    ref_period: int = -1
    pre_trend_fstat: float | None = None
    pre_trend_pval: float | None = None
    n_obs: int = 0
    depvar: str = ""
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        lines = [
            "=== Event Study Results ===",
            f"Dep. variable: {self.depvar}    N = {self.n_obs:,}",
            f"Reference period: {self.ref_period}",
            "",
            f"{'Period':<10} {'Coef':>10} {'Std.Err':>10} {'P>|t|':>8}",
            "-" * 42,
        ]
        for i, t in enumerate(self.periods):
            stars = _significance_stars(self.pval[i])
            lines.append(
                f"{t:<10} {self.coef[i]:>10.4f} {self.se[i]:>10.4f} {self.pval[i]:>8.4f}{stars}"
            )
        lines.append("-" * 42)
        if self.pre_trend_fstat is not None:
            lines.append(
                f"Pre-trend F-test: F = {self.pre_trend_fstat:.3f}, p = {self.pre_trend_pval:.4f}"
            )
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dictionary."""
        return {
            "depvar": self.depvar,
            "n_obs": self.n_obs,
            "ref_period": self.ref_period,
            "pre_trend_fstat": self.pre_trend_fstat,
            "pre_trend_pval": self.pre_trend_pval,
            "periods": {
                t: {
                    "coef": self.coef[i],
                    "se": self.se[i],
                    "pval": self.pval[i],
                    "ci_lower": self.ci_lower[i],
                    "ci_upper": self.ci_upper[i],
                }
                for i, t in enumerate(self.periods)
            },
            "diagnostics": self.diagnostics,
        }


@dataclass
class RDDResult(EstimationResult):
    """Output from a regression discontinuity design estimation.

    Extends EstimationResult with RDD-specific fields.

    Attributes:
        bandwidth: The bandwidth used (optimal or user-specified).
        n_left: Observations to the left of the cutoff (within bandwidth).
        n_right: Observations to the right of the cutoff (within bandwidth).
        cutoff: The cutoff value.
        kernel: Kernel function used.
        polynomial: Polynomial order.
        density_test_pval: P-value from the McCrary density test (None if not run).
    """

    bandwidth: float = 0.0
    n_left: int = 0
    n_right: int = 0
    cutoff: float = 0.0
    kernel: str = "triangular"
    polynomial: int = 1
    density_test_pval: float | None = None

    def __repr__(self) -> str:
        base = super().__repr__()
        rdd_lines = [
            "",
            "RDD-specific:",
            f"  Cutoff: {self.cutoff}    Bandwidth: {self.bandwidth:.4f}",
            f"  N(left): {self.n_left:,}    N(right): {self.n_right:,}",
            f"  Kernel: {self.kernel}    Polynomial order: {self.polynomial}",
        ]
        if self.density_test_pval is not None:
            rdd_lines.append(f"  McCrary density test p-value: {self.density_test_pval:.4f}")
        return base + "\n".join(rdd_lines)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dictionary, including RDD fields."""
        d = super().to_dict()
        d.update(
            {
                "bandwidth": self.bandwidth,
                "n_left": self.n_left,
                "n_right": self.n_right,
                "cutoff": self.cutoff,
                "kernel": self.kernel,
                "polynomial": self.polynomial,
                "density_test_pval": self.density_test_pval,
            }
        )
        return d


def _significance_stars(p: float) -> str:
    """Return significance stars for a p-value."""
    if np.isnan(p):
        return ""
    if p < 0.001:
        return "***"
    if p < 0.01:
        return "**"
    if p < 0.05:
        return "*"
    return ""
