"""Bounding and sensitivity analysis for causal inference.

Implements three canonical bounding approaches:

1. Oster (2019): Bias-adjusted treatment effects under proportional selection.
2. Lee (2009): Trimming bounds for sample selection.
3. Manski (1990): Worst-case (no assumptions) bounds.

These methods are essential for assessing the robustness of causal estimates
to violations of identifying assumptions.

References:
    Oster, E. (2019). Unobservable selection and coefficient stability:
    Theory and evidence. Journal of Business & Economic Statistics, 37(2).

    Lee, D. S. (2009). Training, wages, and sample selection: Estimating
    sharp bounds on treatment effects. Review of Economic Studies, 76(3).

    Manski, C. F. (1990). Nonparametric bounds on treatment effects.
    American Economic Review, 80(2), 319-323.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from .results import EstimationResult


@dataclass
class OsterResult:
    """Output from Oster (2019) bias-adjusted bounds.

    Attributes:
        beta_baseline: Coefficient from the baseline (short) regression.
        beta_full: Coefficient from the full (long) regression.
        r_sq_baseline: R-squared from the baseline regression.
        r_sq_full: R-squared from the full regression.
        r_max: Assumed maximum R-squared under full selection.
        delta: Proportional selection coefficient.
        beta_star: Bias-adjusted coefficient (the bound).
        identified_set: Tuple of (lower, upper) for the identified set.
        breakdown_delta: Value of delta at which the effect becomes zero.
        depvar: Dependent variable name.
        treatment: Treatment variable name.
    """

    beta_baseline: float
    beta_full: float
    r_sq_baseline: float
    r_sq_full: float
    r_max: float
    delta: float
    beta_star: float
    identified_set: tuple[float, float]
    breakdown_delta: float | None
    depvar: str = ""
    treatment: str = ""

    def __repr__(self) -> str:
        lines = [
            "=== Oster (2019) Bounds ===",
            f"Dep. variable: {self.depvar}    Treatment: {self.treatment}",
            "",
            f"Baseline beta: {self.beta_baseline:.4f}  (R2 = {self.r_sq_baseline:.4f})",
            f"Full beta:     {self.beta_full:.4f}  (R2 = {self.r_sq_full:.4f})",
            f"R_max = {self.r_max:.4f}    delta = {self.delta:.4f}",
            "",
            f"Bias-adjusted beta*: {self.beta_star:.4f}",
            f"Identified set: [{self.identified_set[0]:.4f}, {self.identified_set[1]:.4f}]",
        ]
        if self.breakdown_delta is not None:
            lines.append(f"Breakdown delta (beta* = 0): {self.breakdown_delta:.4f}")
        else:
            lines.append("Breakdown delta: could not be computed")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dictionary."""
        return {
            "beta_baseline": self.beta_baseline,
            "beta_full": self.beta_full,
            "r_sq_baseline": self.r_sq_baseline,
            "r_sq_full": self.r_sq_full,
            "r_max": self.r_max,
            "delta": self.delta,
            "beta_star": self.beta_star,
            "identified_set": list(self.identified_set),
            "breakdown_delta": self.breakdown_delta,
        }


@dataclass
class LeeBoundsResult:
    """Output from Lee (2009) trimming bounds.

    Attributes:
        lower_bound: Lower bound of the treatment effect.
        upper_bound: Upper bound of the treatment effect.
        lower_se: Standard error of the lower bound.
        upper_se: Standard error of the upper bound.
        lower_ci: 95% CI for the lower bound (tuple).
        upper_ci: 95% CI for the upper bound (tuple).
        trimming_proportion: Proportion of observations trimmed.
        n_treated: Number of treated observations.
        n_control: Number of control observations.
        depvar: Dependent variable name.
    """

    lower_bound: float
    upper_bound: float
    lower_se: float
    upper_se: float
    lower_ci: tuple[float, float]
    upper_ci: tuple[float, float]
    trimming_proportion: float
    n_treated: int
    n_control: int
    depvar: str = ""

    def __repr__(self) -> str:
        lines = [
            "=== Lee (2009) Bounds ===",
            f"Dep. variable: {self.depvar}",
            f"N(treated) = {self.n_treated}    N(control) = {self.n_control}",
            f"Trimming proportion: {self.trimming_proportion:.4f}",
            "",
            f"Lower bound: {self.lower_bound:.4f} (SE: {self.lower_se:.4f})",
            f"  95% CI: [{self.lower_ci[0]:.4f}, {self.lower_ci[1]:.4f}]",
            f"Upper bound: {self.upper_bound:.4f} (SE: {self.upper_se:.4f})",
            f"  95% CI: [{self.upper_ci[0]:.4f}, {self.upper_ci[1]:.4f}]",
            "",
            f"Identified set: [{self.lower_bound:.4f}, {self.upper_bound:.4f}]",
        ]
        if self.lower_bound > 0:
            lines.append("=> Treatment effect is positive even at the lower bound.")
        elif self.upper_bound < 0:
            lines.append("=> Treatment effect is negative even at the upper bound.")
        else:
            lines.append("=> Identified set includes zero.")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dictionary."""
        return {
            "lower_bound": self.lower_bound,
            "upper_bound": self.upper_bound,
            "lower_se": self.lower_se,
            "upper_se": self.upper_se,
            "lower_ci": list(self.lower_ci),
            "upper_ci": list(self.upper_ci),
            "trimming_proportion": self.trimming_proportion,
        }


@dataclass
class ManskiBoundsResult:
    """Output from Manski (1990) worst-case bounds.

    Attributes:
        lower_bound: Lower bound of the ATE.
        upper_bound: Upper bound of the ATE.
        y_min: Assumed minimum possible value of Y.
        y_max: Assumed maximum possible value of Y.
        prob_treated: Probability of treatment.
        mean_y_treated: Mean of Y among treated.
        mean_y_control: Mean of Y among control.
        n_obs: Total number of observations.
        depvar: Dependent variable name.
    """

    lower_bound: float
    upper_bound: float
    y_min: float
    y_max: float
    prob_treated: float
    mean_y_treated: float
    mean_y_control: float
    n_obs: int
    depvar: str = ""

    def __repr__(self) -> str:
        lines = [
            "=== Manski (1990) Worst-Case Bounds ===",
            f"Dep. variable: {self.depvar}",
            f"N = {self.n_obs}    P(D=1) = {self.prob_treated:.4f}",
            f"Y range: [{self.y_min:.4f}, {self.y_max:.4f}]",
            "",
            f"E[Y|D=1] = {self.mean_y_treated:.4f}",
            f"E[Y|D=0] = {self.mean_y_control:.4f}",
            "",
            f"ATE bounds: [{self.lower_bound:.4f}, {self.upper_bound:.4f}]",
            f"Width: {self.upper_bound - self.lower_bound:.4f}",
        ]
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dictionary."""
        return {
            "lower_bound": self.lower_bound,
            "upper_bound": self.upper_bound,
            "y_min": self.y_min,
            "y_max": self.y_max,
            "width": self.upper_bound - self.lower_bound,
        }


def oster_bounds(
    baseline_result: EstimationResult,
    full_result: EstimationResult,
    treatment: str,
    r_max: float = 1.3,
    delta: float = 1.0,
) -> OsterResult:
    """Compute Oster (2019) bias-adjusted treatment effects.

    Uses the proportional selection assumption: the relationship between
    treatment and unobservables is proportional (by factor delta) to the
    relationship between treatment and observables.

    The key formula:
        beta* = beta_full - delta * (beta_baseline - beta_full) *
                (R_max - R_full) / (R_full - R_baseline)

    Args:
        baseline_result: EstimationResult from a short regression (few controls).
        full_result: EstimationResult from a long regression (more controls).
        treatment: Name of the treatment variable (must be in both results).
        r_max: Maximum R-squared under full selection. Common choices:
            1.3 * R_full (Oster's recommendation) or 1.0.
        delta: Proportional selection coefficient. delta=1 means equal
            selection on observables and unobservables.

    Returns:
        OsterResult with the bias-adjusted coefficient and identified set.
    """
    if treatment not in baseline_result.coef:
        raise ValueError(f"Treatment '{treatment}' not found in baseline_result.coef")
    if treatment not in full_result.coef:
        raise ValueError(f"Treatment '{treatment}' not found in full_result.coef")

    beta_b = baseline_result.coef[treatment]
    beta_f = full_result.coef[treatment]
    r2_b = baseline_result.r_sq
    r2_f = full_result.r_sq

    # If r_max is given as a multiplier (e.g., 1.3), interpret as 1.3 * R_full
    # If r_max > 1 and R_full < 1, it is a multiplier
    if r_max > 1.0 and r2_f < 1.0:
        r_max_actual = min(r_max * r2_f, 1.0)
    else:
        r_max_actual = r_max

    # Oster formula
    denom = r2_f - r2_b
    if abs(denom) < 1e-10:
        # Degenerate case: no change in R2
        beta_star = beta_f
        breakdown = None
    else:
        adjustment = delta * (beta_b - beta_f) * (r_max_actual - r2_f) / denom
        beta_star = beta_f - adjustment

        # Breakdown delta: value of delta where beta* = 0
        # 0 = beta_f - delta_bd * (beta_b - beta_f) * (r_max - r2_f) / (r2_f - r2_b)
        numer_bd = beta_f * denom
        denom_bd = (beta_b - beta_f) * (r_max_actual - r2_f)
        if abs(denom_bd) > 1e-10:
            breakdown = float(numer_bd / denom_bd)
        else:
            breakdown = None

    # Identified set: [min(beta_f, beta*), max(beta_f, beta*)]
    identified_set = (
        min(beta_f, beta_star),
        max(beta_f, beta_star),
    )

    return OsterResult(
        beta_baseline=beta_b,
        beta_full=beta_f,
        r_sq_baseline=r2_b,
        r_sq_full=r2_f,
        r_max=r_max_actual,
        delta=delta,
        beta_star=beta_star,
        identified_set=identified_set,
        breakdown_delta=breakdown,
        depvar=full_result.depvar,
        treatment=treatment,
    )


def lee_bounds(
    df: pd.DataFrame,
    y: str,
    treat_col: str,
    selection_col: str,
    n_bootstrap: int = 500,
    seed: int = 42,
) -> LeeBoundsResult:
    """Compute Lee (2009) trimming bounds for sample selection.

    When treatment affects whether the outcome is observed (sample selection),
    a simple comparison of means is biased. Lee bounds trim the "excess"
    observations in the group with higher selection rates to construct
    sharp bounds on the treatment effect.

    Args:
        df: DataFrame with outcome, treatment, and selection indicator.
        y: Name of the outcome variable (observed only when selected).
        treat_col: Binary treatment column (0/1).
        selection_col: Binary column indicating whether the outcome is
            observed (1) or missing due to selection (0).
        n_bootstrap: Number of bootstrap replications for standard errors.
        seed: Random seed.

    Returns:
        LeeBoundsResult with lower and upper bounds.
    """
    rng = np.random.default_rng(seed)

    data = df[[y, treat_col, selection_col]].copy()

    # Selection rates by treatment status
    sel_treated = data.loc[data[treat_col] == 1, selection_col].mean()
    sel_control = data.loc[data[treat_col] == 0, selection_col].mean()

    # Among selected (observed) units
    obs_treated = data.loc[(data[treat_col] == 1) & (data[selection_col] == 1), y].values
    obs_control = data.loc[(data[treat_col] == 0) & (data[selection_col] == 1), y].values

    n_treated = len(obs_treated)
    n_control = len(obs_control)

    if n_treated == 0 or n_control == 0:
        raise ValueError("Need observed outcomes in both treatment groups.")

    # Trimming proportion: excess selection in the group with higher rate
    if sel_treated >= sel_control:
        # Treatment increases selection; trim from the treated group
        trim_prop = 1.0 - sel_control / sel_treated if sel_treated > 0 else 0.0
        trim_group = "treated"
    else:
        # Treatment decreases selection; trim from the control group
        trim_prop = 1.0 - sel_treated / sel_control if sel_control > 0 else 0.0
        trim_group = "control"

    def _compute_bounds(y_t, y_c, p):
        """Compute bounds for given arrays and trimming proportion."""
        if trim_group == "treated":
            n_trim = int(np.floor(p * len(y_t)))
            if n_trim == 0:
                return float(np.mean(y_t) - np.mean(y_c)), float(np.mean(y_t) - np.mean(y_c))
            sorted_t = np.sort(y_t)
            # Lower bound: trim from the top of treated distribution
            lower = float(np.mean(sorted_t[: len(y_t) - n_trim]) - np.mean(y_c))
            # Upper bound: trim from the bottom of treated distribution
            upper = float(np.mean(sorted_t[n_trim:]) - np.mean(y_c))
        else:
            n_trim = int(np.floor(p * len(y_c)))
            if n_trim == 0:
                return float(np.mean(y_t) - np.mean(y_c)), float(np.mean(y_t) - np.mean(y_c))
            sorted_c = np.sort(y_c)
            # Lower bound: trim from the bottom of control distribution
            lower = float(np.mean(y_t) - np.mean(sorted_c[n_trim:]))
            # Upper bound: trim from the top of control distribution
            upper = float(np.mean(y_t) - np.mean(sorted_c[: len(y_c) - n_trim]))
        return lower, upper

    lower_bound, upper_bound = _compute_bounds(obs_treated, obs_control, trim_prop)

    # Bootstrap SE
    boot_lowers = np.zeros(n_bootstrap)
    boot_uppers = np.zeros(n_bootstrap)

    for b in range(n_bootstrap):
        idx_t = rng.choice(n_treated, size=n_treated, replace=True)
        idx_c = rng.choice(n_control, size=n_control, replace=True)
        bl, bu = _compute_bounds(obs_treated[idx_t], obs_control[idx_c], trim_prop)
        boot_lowers[b] = bl
        boot_uppers[b] = bu

    lower_se = float(np.std(boot_lowers, ddof=1))
    upper_se = float(np.std(boot_uppers, ddof=1))

    lower_ci = (lower_bound - 1.96 * lower_se, lower_bound + 1.96 * lower_se)
    upper_ci = (upper_bound - 1.96 * upper_se, upper_bound + 1.96 * upper_se)

    return LeeBoundsResult(
        lower_bound=lower_bound,
        upper_bound=upper_bound,
        lower_se=lower_se,
        upper_se=upper_se,
        lower_ci=lower_ci,
        upper_ci=upper_ci,
        trimming_proportion=trim_prop,
        n_treated=n_treated,
        n_control=n_control,
        depvar=y,
    )


def manski_bounds(
    df: pd.DataFrame,
    y: str,
    treat_col: str,
    y_min: float | None = None,
    y_max: float | None = None,
) -> ManskiBoundsResult:
    """Compute Manski (1990) worst-case bounds on the ATE.

    Under no assumptions about selection, the ATE is bounded by replacing
    missing potential outcomes with the extreme possible values of Y.

    Args:
        df: DataFrame with outcome and treatment columns.
        y: Name of the outcome variable.
        treat_col: Binary treatment column (0/1).
        y_min: Minimum possible value of Y. If None, uses observed min.
        y_max: Maximum possible value of Y. If None, uses observed max.

    Returns:
        ManskiBoundsResult with the worst-case bounds.
    """
    data = df[[y, treat_col]].dropna()
    y_vals = data[y].values.astype(float)
    treat_vals = data[treat_col].values.astype(int)

    if y_min is None:
        y_min = float(np.min(y_vals))
    if y_max is None:
        y_max = float(np.max(y_vals))

    mean_y1 = float(np.mean(y_vals[treat_vals == 1]))
    mean_y0 = float(np.mean(y_vals[treat_vals == 0]))
    p = float(np.mean(treat_vals))
    n = len(data)

    # Manski bounds on ATE
    ey1_lower = mean_y1 * p + y_min * (1 - p)
    ey1_upper = mean_y1 * p + y_max * (1 - p)
    ey0_lower = mean_y0 * (1 - p) + y_min * p
    ey0_upper = mean_y0 * (1 - p) + y_max * p

    ate_lower = ey1_lower - ey0_upper
    ate_upper = ey1_upper - ey0_lower

    return ManskiBoundsResult(
        lower_bound=ate_lower,
        upper_bound=ate_upper,
        y_min=y_min,
        y_max=y_max,
        prob_treated=p,
        mean_y_treated=mean_y1,
        mean_y_control=mean_y0,
        n_obs=n,
        depvar=y,
    )
