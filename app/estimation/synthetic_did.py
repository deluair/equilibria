"""Synthetic Difference-in-Differences (Arkhangelsky et al., 2021).

Implements the SDID estimator, which combines the strengths of synthetic
control and difference-in-differences by re-weighting both units and
time periods to construct a more appropriate counterfactual.

References:
    Arkhangelsky, D., Athey, S., Hirshberg, D. A., Imbens, G. W., &
    Wager, S. (2021). Synthetic difference-in-differences. American
    Economic Review, 111(12), 4088-4118.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from scipy.optimize import minimize

try:
    import matplotlib.pyplot as plt

    _HAS_MPL = True
except ImportError:
    _HAS_MPL = False


@dataclass
class SDIDResult:
    """Output from a synthetic DID estimation.

    Attributes:
        att: Average treatment effect on the treated.
        se: Standard error (placebo-based or bootstrap).
        ci_lower: Lower bound of 95% confidence interval.
        ci_upper: Upper bound of 95% confidence interval.
        unit_weights: Dictionary mapping control unit IDs to their weights.
        time_weights: Dictionary mapping pre-treatment periods to their weights.
        n_treated: Number of treated units.
        n_control: Number of control units.
        n_pre: Number of pre-treatment periods.
        n_post: Number of post-treatment periods.
        depvar: Name of the outcome variable.
        diagnostics: Additional diagnostics.
    """

    att: float
    se: float
    ci_lower: float
    ci_upper: float
    unit_weights: dict[Any, float]
    time_weights: dict[Any, float]
    n_treated: int
    n_control: int
    n_pre: int
    n_post: int
    depvar: str = ""
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        lines = [
            "=== Synthetic DID Results ===",
            f"Dep. variable: {self.depvar}",
            f"N(treated): {self.n_treated}    N(control): {self.n_control}",
            f"T(pre): {self.n_pre}    T(post): {self.n_post}",
            "",
            f"ATT: {self.att:.4f} (SE: {self.se:.4f})",
            f"95% CI: [{self.ci_lower:.4f}, {self.ci_upper:.4f}]",
            "",
            "Top unit weights (control):",
        ]
        sorted_uw = sorted(self.unit_weights.items(), key=lambda x: -x[1])
        for uid, w in sorted_uw[:10]:
            if w > 0.001:
                lines.append(f"  {uid}: {w:.4f}")
        lines.append("")
        lines.append("Time weights (pre-treatment):")
        sorted_tw = sorted(self.time_weights.items(), key=lambda x: x[0])
        for t, w in sorted_tw:
            if w > 0.001:
                lines.append(f"  {t}: {w:.4f}")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dictionary."""
        return {
            "att": self.att,
            "se": self.se,
            "ci_lower": self.ci_lower,
            "ci_upper": self.ci_upper,
            "unit_weights": self.unit_weights,
            "time_weights": self.time_weights,
            "n_treated": self.n_treated,
            "n_control": self.n_control,
            "n_pre": self.n_pre,
            "n_post": self.n_post,
        }


def _solve_unit_weights(
    Y_control_pre: np.ndarray,
    Y_treated_pre: np.ndarray,
    zeta: float,
) -> np.ndarray:
    """Find unit (omega) weights for control units.

    Minimizes the squared distance between the weighted control average
    and the treated average in pre-treatment outcomes, with an L2 penalty
    controlled by zeta.

    Args:
        Y_control_pre: (N_co x T_pre) matrix of control unit pre-treatment outcomes.
        Y_treated_pre: (T_pre,) vector of treated average pre-treatment outcomes.
        zeta: Regularization parameter.

    Returns:
        Array of weights for control units (sums to 1, non-negative).
    """
    n_co = Y_control_pre.shape[0]

    def objective(w):
        synth = w @ Y_control_pre  # weighted average of controls
        fit_term = np.sum((synth - Y_treated_pre) ** 2)
        penalty = zeta * n_co * np.sum(w**2)
        return fit_term + penalty

    # Constraints: weights sum to 1
    constraints = {"type": "eq", "fun": lambda w: np.sum(w) - 1.0}
    # Bounds: non-negative weights
    bounds = [(0.0, None)] * n_co
    w0 = np.ones(n_co) / n_co

    result = minimize(
        objective,
        w0,
        method="SLSQP",
        bounds=bounds,
        constraints=constraints,
        options={"maxiter": 1000, "ftol": 1e-12},
    )
    return result.x


def _solve_time_weights(
    Y_control_pre: np.ndarray,
    Y_control_post: np.ndarray,
    zeta: float,
) -> np.ndarray:
    """Find time (lambda) weights for pre-treatment periods.

    Minimizes the squared distance between the weighted pre-treatment
    control average and the post-treatment control average across units,
    with an L2 penalty.

    Args:
        Y_control_pre: (N_co x T_pre) matrix.
        Y_control_post: (N_co,) vector of control post-treatment averages.
        zeta: Regularization parameter.

    Returns:
        Array of time weights (sums to 1, non-negative).
    """
    t_pre = Y_control_pre.shape[1]

    def objective(lam):
        synth = Y_control_pre @ lam  # per-unit weighted pre-treatment average
        fit_term = np.sum((synth - Y_control_post) ** 2)
        penalty = zeta * t_pre * np.sum(lam**2)
        return fit_term + penalty

    constraints = {"type": "eq", "fun": lambda lam: np.sum(lam) - 1.0}
    bounds = [(0.0, None)] * t_pre
    lam0 = np.ones(t_pre) / t_pre

    result = minimize(
        objective,
        lam0,
        method="SLSQP",
        bounds=bounds,
        constraints=constraints,
        options={"maxiter": 1000, "ftol": 1e-12},
    )
    return result.x


def _compute_zeta(Y_control_pre: np.ndarray) -> float:
    """Compute the regularization parameter zeta as in Arkhangelsky et al.

    Uses the standard deviation of first differences in the control
    pre-treatment panel, scaled by the number of pre-treatment periods.

    Args:
        Y_control_pre: (N_co x T_pre) matrix.

    Returns:
        Regularization parameter zeta.
    """
    n_co, t_pre = Y_control_pre.shape
    if t_pre < 2:
        return 1.0
    # First differences along time
    diffs = Y_control_pre[:, 1:] - Y_control_pre[:, :-1]
    sigma = np.std(diffs)
    # Arkhangelsky et al. suggest zeta = (N_co * T_pre)^{1/4} * sigma
    zeta = (n_co * t_pre) ** 0.25 * sigma
    return max(zeta, 1e-6)


def run_sdid(
    df: pd.DataFrame,
    y: str,
    entity_col: str,
    time_col: str,
    treat_col: str,
    post_col: str,
    n_placebo: int = 200,
    seed: int = 42,
) -> SDIDResult:
    """Estimate the ATT using Synthetic Difference-in-Differences.

    Args:
        df: Balanced panel DataFrame.
        y: Name of the outcome variable.
        entity_col: Column identifying panel units (e.g., 'state').
        time_col: Column identifying time periods (e.g., 'year').
        treat_col: Binary column: 1 if the unit is ever treated, 0 otherwise.
        post_col: Binary column: 1 if the period is post-treatment, 0 otherwise.
        n_placebo: Number of placebo permutations for inference.
        seed: Random seed.

    Returns:
        SDIDResult with ATT, standard error, and weights.

    Raises:
        ValueError: If the panel is not balanced or inputs are invalid.
    """
    rng = np.random.default_rng(seed)

    # Validate and reshape into panel matrix
    panel = df.pivot_table(index=entity_col, columns=time_col, values=y)
    if panel.isnull().any().any():
        raise ValueError(
            "Panel is not balanced. Every entity must have a value for every "
            "time period. Fill missing values before calling run_sdid."
        )

    # Identify treated and control units
    treat_map = df.groupby(entity_col)[treat_col].max()
    treated_ids = treat_map[treat_map == 1].index.tolist()
    control_ids = treat_map[treat_map == 0].index.tolist()

    if len(treated_ids) == 0 or len(control_ids) == 0:
        raise ValueError("Need at least one treated and one control unit.")

    # Identify pre and post periods
    time_map = df.groupby(time_col)[post_col].max()
    pre_periods = sorted(time_map[time_map == 0].index.tolist())
    post_periods = sorted(time_map[time_map == 1].index.tolist())

    if len(pre_periods) == 0 or len(post_periods) == 0:
        raise ValueError("Need at least one pre-treatment and one post-treatment period.")

    # Build matrices
    Y_control_pre = panel.loc[control_ids, pre_periods].values  # (N_co x T_pre)
    Y_control_post = panel.loc[control_ids, post_periods].values  # (N_co x T_post)
    Y_treated_pre = panel.loc[treated_ids, pre_periods].values  # (N_tr x T_pre)
    Y_treated_post = panel.loc[treated_ids, post_periods].values  # (N_tr x T_post)

    # Averages for treated units
    Y_tr_pre_avg = Y_treated_pre.mean(axis=0)  # (T_pre,)
    Y_co_post_avg = Y_control_post.mean(axis=1)  # (N_co,)

    # Compute regularization parameter
    zeta = _compute_zeta(Y_control_pre)

    # Solve for weights
    omega = _solve_unit_weights(Y_control_pre, Y_tr_pre_avg, zeta)
    lam = _solve_time_weights(Y_control_pre, Y_co_post_avg, zeta)

    # SDID estimator: double-weighted DiD
    # ATT = (Y_tr_post - omega' Y_co_post) - lambda' (Y_tr_pre - omega' Y_co_pre)
    Y_tr_post_avg = Y_treated_post.mean(axis=0)  # (T_post,)

    # Post-treatment differences
    post_diff = Y_tr_post_avg.mean() - (omega @ Y_control_post).mean()

    # Pre-treatment differences (time-weighted)
    pre_diff_treated = lam @ Y_tr_pre_avg
    pre_diff_control = lam @ (omega @ Y_control_pre)
    pre_diff = pre_diff_treated - pre_diff_control

    att = post_diff - pre_diff

    # Placebo inference: permute treatment assignment among control units
    # (if we have enough control units) or use jackknife
    placebo_atts = []
    n_co = len(control_ids)

    if n_co >= 2 * len(treated_ids):
        # Permutation-based inference
        for _ in range(n_placebo):
            perm = rng.permutation(n_co)
            n_fake_treat = len(treated_ids)
            fake_treat_idx = perm[:n_fake_treat]
            fake_control_idx = perm[n_fake_treat:]

            # Recompute with permuted assignment
            Y_fco_pre = Y_control_pre[fake_control_idx]
            Y_fco_post = Y_control_post[fake_control_idx]
            Y_ftr_pre = Y_control_pre[fake_treat_idx]
            Y_ftr_post = Y_control_post[fake_treat_idx]

            ftr_pre_avg = Y_ftr_pre.mean(axis=0)
            fco_post_avg = Y_fco_post.mean(axis=1)

            fzeta = _compute_zeta(Y_fco_pre)
            fomega = _solve_unit_weights(Y_fco_pre, ftr_pre_avg, fzeta)
            flam = _solve_time_weights(Y_fco_pre, fco_post_avg, fzeta)

            fpost_diff = Y_ftr_post.mean(axis=0).mean() - (fomega @ Y_fco_post).mean()
            fpre_diff = flam @ ftr_pre_avg - flam @ (fomega @ Y_fco_pre)
            placebo_atts.append(fpost_diff - fpre_diff)
    else:
        # Jackknife-based inference when too few controls
        for i in range(n_co):
            jk_idx = [j for j in range(n_co) if j != i]
            Y_jco_pre = Y_control_pre[jk_idx]
            Y_jco_post = Y_control_post[jk_idx]

            jzeta = _compute_zeta(Y_jco_pre)
            jomega = _solve_unit_weights(Y_jco_pre, Y_tr_pre_avg, jzeta)
            jlam = _solve_time_weights(Y_jco_pre, Y_control_post[jk_idx].mean(axis=1), jzeta)

            jpost_diff = Y_tr_post_avg.mean() - (jomega @ Y_jco_post).mean()
            jpre_diff = jlam @ Y_tr_pre_avg - jlam @ (jomega @ Y_jco_pre)
            placebo_atts.append(jpost_diff - jpre_diff)

    placebo_atts = np.array(placebo_atts)
    se = float(np.std(placebo_atts, ddof=1))
    if se < 1e-10:
        se = np.nan
    ci_lower = att - 1.96 * se
    ci_upper = att + 1.96 * se

    # Build weight dictionaries
    unit_weight_dict = {control_ids[i]: float(omega[i]) for i in range(n_co)}
    time_weight_dict = {pre_periods[t]: float(lam[t]) for t in range(len(pre_periods))}

    return SDIDResult(
        att=float(att),
        se=se,
        ci_lower=ci_lower,
        ci_upper=ci_upper,
        unit_weights=unit_weight_dict,
        time_weights=time_weight_dict,
        n_treated=len(treated_ids),
        n_control=n_co,
        n_pre=len(pre_periods),
        n_post=len(post_periods),
        depvar=y,
        diagnostics={
            "zeta": zeta,
            "n_placebo": n_placebo,
            "inference_method": "permutation" if n_co >= 2 * len(treated_ids) else "jackknife",
            "placebo_atts": placebo_atts,
        },
    )


def plot_sdid(
    result: SDIDResult,
    df: pd.DataFrame,
    y: str,
    entity_col: str,
    time_col: str,
    treat_col: str,
    treated_label: str | None = None,
    synthetic_label: str | None = None,
    ax=None,
) -> Any:
    """Plot treated vs. synthetic control unit over time.

    Args:
        result: Output from run_sdid.
        df: The original panel DataFrame.
        y: Name of the outcome variable.
        entity_col: Column identifying panel units.
        time_col: Column identifying time periods.
        treat_col: Binary column for treatment status.
        treated_label: Label for the treated line.
        synthetic_label: Label for the synthetic control line.
        ax: Optional matplotlib axes.

    Returns:
        Matplotlib axes object.

    Raises:
        ImportError: If matplotlib is not installed.
    """
    if not _HAS_MPL:
        raise ImportError("matplotlib is required for plotting. Install with: uv add matplotlib")

    if ax is None:
        fig, ax = plt.subplots(figsize=(10, 6))

    panel = df.pivot_table(index=entity_col, columns=time_col, values=y)
    treat_map = df.groupby(entity_col)[treat_col].max()
    treated_ids = treat_map[treat_map == 1].index.tolist()
    control_ids = treat_map[treat_map == 0].index.tolist()

    times = sorted(panel.columns)

    # Treated average
    treated_ts = panel.loc[treated_ids].mean(axis=0).loc[times]

    # Synthetic control: weighted average of controls using SDID unit weights
    weights = np.array([result.unit_weights.get(uid, 0.0) for uid in control_ids])
    synth_ts = weights @ panel.loc[control_ids].values
    synth_ts = pd.Series(synth_ts, index=panel.columns).loc[times]

    if treated_label is None:
        treated_label = "Treated"
    if synthetic_label is None:
        synthetic_label = "Synthetic control (SDID)"

    ax.plot(times, treated_ts.values, "o-", color="steelblue", label=treated_label)
    ax.plot(times, synth_ts.values, "s--", color="coral", label=synthetic_label)

    # Mark the treatment onset
    pre_periods = [t for t, w in result.time_weights.items()]
    if pre_periods:
        last_pre = max(pre_periods)
        ax.axvline(x=last_pre, color="gray", linestyle=":", alpha=0.7, label="Treatment onset")

    ax.set_xlabel(time_col)
    ax.set_ylabel(y)
    ax.set_title("Synthetic DID: Treated vs. Synthetic Control")
    ax.legend()

    return ax
