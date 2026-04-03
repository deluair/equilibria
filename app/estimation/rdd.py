"""Regression Discontinuity Design estimation.

Provides local linear regression with optimal bandwidth selection,
placebo cutoff tests, and McCrary density tests.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy import stats

from .results import RDDResult


def run_rdd(
    df: pd.DataFrame,
    y: str,
    running_var: str,
    cutoff: float = 0.0,
    bandwidth: float | None = None,
    kernel: str = "triangular",
    polynomial: int = 1,
    cluster: str | None = None,
    alpha: float = 0.05,
) -> RDDResult:
    """Run a sharp regression discontinuity design.

    Uses local polynomial regression around the cutoff. If no bandwidth
    is provided, an optimal bandwidth is selected using the IK method.

    Parameters
    ----------
    df : DataFrame
        Input data.
    y : str
        Outcome variable.
    running_var : str
        Running (forcing) variable.
    cutoff : float
        Cutoff value (default 0).
    bandwidth : float, optional
        Bandwidth for local estimation. If None, computed optimally.
    kernel : str
        Kernel function: "triangular", "epanechnikov", or "uniform".
    polynomial : int
        Order of the local polynomial (1 = linear, 2 = quadratic).
    cluster : str, optional
        Cluster variable for standard errors.
    alpha : float
        Significance level.

    Returns
    -------
    RDDResult
        Includes bandwidth, left/right counts, and diagnostic info.
    """
    cols = [y, running_var]
    if cluster and cluster not in cols:
        cols.append(cluster)

    working = df[cols].dropna().copy()

    # Center the running variable at the cutoff
    working["_rv_centered"] = working[running_var] - cutoff

    # Compute optimal bandwidth if not provided
    if bandwidth is None:
        bandwidth = _ik_bandwidth(working[y].values, working["_rv_centered"].values)

    # Subset to bandwidth window
    mask = working["_rv_centered"].abs() <= bandwidth
    local = working[mask].copy()

    if len(local) < 10:
        raise ValueError(
            f"Only {len(local)} observations within bandwidth {bandwidth:.4f}. "
            "Try a wider bandwidth or check your data."
        )

    # Treatment indicator
    local["_treat"] = (local["_rv_centered"] >= 0).astype(float)

    # Kernel weights
    local["_weight"] = _kernel_weights(local["_rv_centered"].values, bandwidth, kernel)

    # Build design matrix: polynomial terms, interacted with treatment
    rv = local["_rv_centered"]
    parts = [local[["_treat"]]]
    for p in range(1, polynomial + 1):
        col_below = f"_rv_pow{p}"
        col_above = f"_rv_treat_pow{p}"
        local[col_below] = rv**p
        local[col_above] = local["_treat"] * rv**p
        parts.append(local[[col_below, col_above]])

    X = pd.concat(parts, axis=1)
    X = sm.add_constant(X, has_constant="add")
    Y = local[y]
    W = local["_weight"]

    model = sm.WLS(Y, X, weights=W)
    if cluster is not None:
        result = model.fit(cov_type="cluster", cov_kwds={"groups": local[cluster]}, use_t=True)
    else:
        result = model.fit(cov_type="HC1", use_t=True)

    # The treatment effect is the coefficient on _treat
    tau = float(result.params["_treat"])
    tau_se = float(result.bse["_treat"])
    tau_pval = float(result.pvalues["_treat"])
    ci = result.conf_int(alpha=alpha)
    tau_ci_lo = float(ci.loc["_treat", 0])
    tau_ci_hi = float(ci.loc["_treat", 1])

    n_left = int((local["_treat"] == 0).sum())
    n_right = int((local["_treat"] == 1).sum())

    diag: dict = {
        "bandwidth_method": "user-specified" if bandwidth is not None else "IK optimal",
    }
    if cluster:
        diag["n_clusters"] = int(local[cluster].nunique())

    return RDDResult(
        coef={"treatment_effect": tau},
        se={"treatment_effect": tau_se},
        pval={"treatment_effect": tau_pval},
        ci_lower={"treatment_effect": tau_ci_lo},
        ci_upper={"treatment_effect": tau_ci_hi},
        n_obs=len(local),
        r_sq=float(result.rsquared),
        method="RDD",
        depvar=y,
        diagnostics=diag,
        bandwidth=bandwidth,
        n_left=n_left,
        n_right=n_right,
        cutoff=cutoff,
        kernel=kernel,
        polynomial=polynomial,
    )


def placebo_cutoff_test(
    df: pd.DataFrame,
    y: str,
    running_var: str,
    true_cutoff: float = 0.0,
    placebo_cutoffs: list[float] | None = None,
    bandwidth: float | None = None,
    kernel: str = "triangular",
    polynomial: int = 1,
) -> pd.DataFrame:
    """Run RDD at placebo cutoff points to check for spurious discontinuities.

    Parameters
    ----------
    df : DataFrame
    y : str
    running_var : str
    true_cutoff : float
    placebo_cutoffs : list of float, optional
        If None, uses 5 equally spaced points on each side of the true cutoff.
    bandwidth : float, optional
    kernel : str
    polynomial : int

    Returns
    -------
    DataFrame with columns: cutoff, coef, se, pval, significant
    """
    if placebo_cutoffs is None:
        rv = df[running_var].dropna()
        lo, hi = rv.quantile(0.1), rv.quantile(0.9)
        placebo_cutoffs = np.linspace(lo, hi, 11).tolist()
        # Remove any point too close to the true cutoff
        placebo_cutoffs = [c for c in placebo_cutoffs if abs(c - true_cutoff) > (hi - lo) * 0.05]

    rows = []
    for c in placebo_cutoffs:
        try:
            res = run_rdd(
                df,
                y,
                running_var,
                cutoff=c,
                bandwidth=bandwidth,
                kernel=kernel,
                polynomial=polynomial,
            )
            rows.append(
                {
                    "cutoff": c,
                    "coef": res.coef["treatment_effect"],
                    "se": res.se["treatment_effect"],
                    "pval": res.pval["treatment_effect"],
                    "significant": res.pval["treatment_effect"] < 0.05,
                }
            )
        except (ValueError, np.linalg.LinAlgError):
            continue

    return pd.DataFrame(rows)


def mccrary_density_test(
    df: pd.DataFrame,
    running_var: str,
    cutoff: float = 0.0,
    n_bins: int = 50,
) -> dict:
    """McCrary (2008) density test for manipulation of the running variable.

    Tests whether there is a discontinuity in the density of the running
    variable at the cutoff (which would suggest manipulation).

    This is a simplified implementation using a local linear density estimator.

    Parameters
    ----------
    df : DataFrame
    running_var : str
    cutoff : float
    n_bins : int
        Number of bins for histogram estimation.

    Returns
    -------
    dict with: stat, pval, log_diff (log difference in density at cutoff)
    """
    rv = df[running_var].dropna().values
    centered = rv - cutoff

    # Bin the data
    bin_width = (centered.max() - centered.min()) / n_bins
    bins = np.arange(centered.min(), centered.max() + bin_width, bin_width)
    counts, edges = np.histogram(centered, bins=bins)
    midpoints = (edges[:-1] + edges[1:]) / 2

    # Normalize to density
    density = counts / (len(rv) * bin_width)

    # Separate left and right of cutoff
    left_mask = midpoints < 0
    right_mask = midpoints >= 0

    if left_mask.sum() < 3 or right_mask.sum() < 3:
        return {
            "stat": np.nan,
            "pval": np.nan,
            "log_diff": np.nan,
            "message": "Too few bins on one side of the cutoff",
        }

    # Fit local linear on each side, extrapolate to cutoff
    # Left side
    X_left = sm.add_constant(midpoints[left_mask])
    model_left = sm.OLS(np.log(density[left_mask] + 1e-10), X_left).fit()
    f_left = float(np.exp(model_left.predict(np.array([[1.0, 0.0]]))))

    # Right side
    X_right = sm.add_constant(midpoints[right_mask])
    model_right = sm.OLS(np.log(density[right_mask] + 1e-10), X_right).fit()
    f_right = float(np.exp(model_right.predict(np.array([[1.0, 0.0]]))))

    log_diff = np.log(f_right + 1e-10) - np.log(f_left + 1e-10)

    # Approximate standard error via delta method
    se_left = float(model_left.bse[0]) * f_left
    se_right = float(model_right.bse[0]) * f_right
    se_diff = np.sqrt(se_left**2 + se_right**2) / ((f_left + f_right) / 2 + 1e-10)

    t_stat = log_diff / (se_diff + 1e-10)
    p_val = 2.0 * (1.0 - stats.norm.cdf(abs(t_stat)))

    return {
        "stat": float(t_stat),
        "pval": float(p_val),
        "log_diff": float(log_diff),
        "density_left": f_left,
        "density_right": f_right,
    }


def _kernel_weights(x: np.ndarray, bandwidth: float, kernel: str) -> np.ndarray:
    """Compute kernel weights for observations within bandwidth."""
    u = x / bandwidth
    if kernel == "triangular":
        w = np.maximum(1.0 - np.abs(u), 0.0)
    elif kernel == "epanechnikov":
        w = np.maximum(0.75 * (1.0 - u**2), 0.0)
    elif kernel == "uniform":
        w = np.ones_like(u)
    else:
        raise ValueError(f"Unknown kernel: {kernel}. Use triangular, epanechnikov, or uniform.")
    return w


def _ik_bandwidth(y: np.ndarray, x: np.ndarray) -> float:
    """Compute Imbens-Kalyanaraman (2012) optimal bandwidth.

    This is a simplified version of the IK procedure for local linear RDD.
    For production work, consider using the rdrobust package directly.
    """
    n = len(y)

    # Step 1: pilot bandwidth using Silverman's rule of thumb
    h_pilot = 1.06 * np.std(x) * n ** (-0.2)

    # Step 2: estimate second derivatives on each side
    left = x < 0
    right = x >= 0

    def _local_quadratic_curvature(mask: np.ndarray, h: float) -> float:
        """Estimate curvature (second derivative) using local quadratic."""
        xi = x[mask]
        yi = y[mask]
        w = _kernel_weights(xi, h, "triangular")
        X = np.column_stack([np.ones(len(xi)), xi, xi**2])
        try:
            Xw = X * w[:, None]
            beta = np.linalg.lstsq(Xw, yi * w, rcond=None)[0]
            return 2.0 * beta[2]  # second derivative
        except (np.linalg.LinAlgError, IndexError):
            return 0.0

    m2_left = _local_quadratic_curvature(left, h_pilot)
    m2_right = _local_quadratic_curvature(right, h_pilot)

    # Step 3: regularization constants
    n_left = left.sum()
    n_right = right.sum()

    # Variance estimation
    var_left = np.var(y[left]) if n_left > 1 else 1.0
    var_right = np.var(y[right]) if n_right > 1 else 1.0

    # Step 4: compute optimal bandwidth
    C_k = 3.4375  # constant for triangular kernel, local linear
    curvature = m2_right - m2_left
    if abs(curvature) < 1e-10:
        curvature = 1e-10

    regularization = var_left / n_left + var_right / n_right
    h_opt = C_k * (regularization / (curvature**2 + 1e-10)) ** 0.2

    # Bound the bandwidth to something reasonable
    x_range = np.ptp(x)
    h_opt = np.clip(h_opt, x_range * 0.02, x_range * 0.5)

    return float(h_opt)
