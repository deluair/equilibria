"""Instrumental Variables / 2SLS estimation wrapper around linearmodels.

Reports first-stage F-statistic, weak instrument diagnostics,
overidentification tests, and Anderson-Rubin weak-IV-robust inference.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from linearmodels.iv import IV2SLS
from scipy import stats

from .results import EstimationResult


@dataclass
class AndersonRubinResult:
    """Result from Anderson-Rubin weak-instrument-robust inference.

    Attributes:
        ci_lower: Lower bound of the AR confidence set (may be -inf).
        ci_upper: Upper bound of the AR confidence set (may be +inf).
        alpha: Significance level used.
        grid_min: Lower bound of the search grid.
        grid_max: Upper bound of the search grid.
        grid_points: Number of grid points evaluated.
        unbounded: True if the confidence set is (-inf, +inf).
        n_obs: Number of observations used.
    """

    ci_lower: float
    ci_upper: float
    alpha: float
    grid_min: float
    grid_max: float
    grid_points: int
    unbounded: bool
    n_obs: int

    def __repr__(self) -> str:
        if self.unbounded:
            return f"AR {int((1 - self.alpha) * 100)}% CI: (-inf, +inf) [unbounded]"
        lo = f"{self.ci_lower:.3f}" if np.isfinite(self.ci_lower) else "-inf"
        hi = f"{self.ci_upper:.3f}" if np.isfinite(self.ci_upper) else "+inf"
        return f"AR {int((1 - self.alpha) * 100)}% CI: ({lo}, {hi})"


def _iterative_demean(
    arrays: list[np.ndarray],
    group_ids: list[np.ndarray],
    max_iter: int = 100,
    tol: float = 1e-8,
) -> list[np.ndarray]:
    """Demean arrays by multiple groups via iterative projection.

    Used to absorb multiple fixed effects (e.g., country + year) without
    constructing a full dummy matrix.

    Args:
        arrays: List of 1-D arrays to demean.
        group_ids: List of integer group-ID arrays (one per FE dimension).
        max_iter: Maximum iterations for convergence.
        tol: Convergence tolerance on the max change.

    Returns:
        List of demeaned arrays, same shape as inputs.
    """
    result = [a.copy().astype(float) for a in arrays]
    for _ in range(max_iter):
        max_change = 0.0
        for gid in group_ids:
            for arr in result:
                means = np.zeros_like(arr)
                np.add.at(means, gid, arr)
                counts = np.zeros_like(arr)
                np.add.at(counts, gid, 1.0)
                counts[counts == 0] = 1.0
                group_mean = means[gid] / counts[gid]
                change = np.max(np.abs(group_mean))
                max_change = max(max_change, change)
                arr -= group_mean
        if max_change < tol:
            break
    return result


def anderson_rubin_ci(
    df: pd.DataFrame,
    y: str,
    endog: str,
    instruments: list[str],
    controls: list[str] | None = None,
    fe_vars: list[str] | None = None,
    cluster: str | None = None,
    alpha: float = 0.05,
    grid_min: float = -5.0,
    grid_max: float = 5.0,
    grid_points: int = 2000,
) -> AndersonRubinResult:
    """Compute an Anderson-Rubin (1949) weak-instrument-robust confidence set.

    For each candidate value beta_0 on a grid, forms y_tilde = y - beta_0 * endog,
    regresses y_tilde on the instruments (after absorbing FE and controls), and
    tests the joint significance of the instruments via an F-test. The AR confidence
    set is the set of beta_0 values where the F-test does not reject at level alpha.

    This provides valid inference regardless of instrument strength, unlike
    standard Wald-based confidence intervals which require strong instruments.

    Parameters
    ----------
    df : DataFrame
        Input data.
    y : str
        Dependent variable name.
    endog : str
        Single endogenous regressor name.
    instruments : list of str
        Excluded instrument variable names.
    controls : list of str, optional
        Exogenous control variable names.
    fe_vars : list of str, optional
        Fixed effect variables (absorbed via iterative demeaning).
    cluster : str, optional
        Cluster variable for clustered F-test.
    alpha : float
        Significance level (default 0.05 for 95% CI).
    grid_min : float
        Lower bound of the search grid for beta_0.
    grid_max : float
        Upper bound of the search grid for beta_0.
    grid_points : int
        Number of points in the search grid.

    Returns
    -------
    AndersonRubinResult
        Contains the CI bounds and metadata.

    References
    ----------
    Anderson, T. W. and Rubin, H. (1949). Estimation of the Parameters of a
    Single Equation in a Complete System of Stochastic Equations.
    Annals of Mathematical Statistics, 20(1), 46-63.
    """
    cols = [y, endog] + instruments
    if controls:
        cols += controls
    if fe_vars:
        cols += fe_vars
    if cluster:
        cols.append(cluster)

    working = df[list(set(cols))].dropna().copy()
    n = len(working)
    k = len(instruments)

    # Encode FE groups as integer IDs
    group_ids = []
    if fe_vars:
        for fv in fe_vars:
            codes, _ = pd.factorize(working[fv])
            group_ids.append(codes)

    # Get cluster IDs if clustering
    cluster_ids = None
    if cluster:
        cluster_ids, _ = pd.factorize(working[cluster])

    # Extract raw arrays
    y_raw = working[y].values.astype(float)
    endog_raw = working[endog].values.astype(float)
    z_raw = working[instruments].values.astype(float)
    ctrl_raw = working[controls].values.astype(float) if controls else None

    grid = np.linspace(grid_min, grid_max, grid_points)
    f_crit = stats.f.ppf(1 - alpha, k, n - k - 1)
    accepted = np.zeros(grid_points, dtype=bool)

    for gi, beta0 in enumerate(grid):
        y_tilde = y_raw - beta0 * endog_raw

        # Demean by FE
        to_demean = [y_tilde.copy()] + [z_raw[:, j].copy() for j in range(k)]
        if ctrl_raw is not None:
            for j in range(ctrl_raw.shape[1]):
                to_demean.append(ctrl_raw[:, j].copy())

        if group_ids:
            demeaned = _iterative_demean(to_demean, group_ids)
        else:
            demeaned = to_demean

        yt = demeaned[0]
        zt = np.column_stack(demeaned[1 : 1 + k])

        # Partial out controls from both yt and zt
        if ctrl_raw is not None:
            ct = np.column_stack(demeaned[1 + k :])
            # Project out controls
            try:
                proj = ct @ np.linalg.lstsq(ct, yt, rcond=None)[0]
                yt = yt - proj
                for j in range(k):
                    proj_z = ct @ np.linalg.lstsq(ct, zt[:, j], rcond=None)[0]
                    zt[:, j] = zt[:, j] - proj_z
            except np.linalg.LinAlgError:
                pass

        # F-test: regress yt on zt
        try:
            beta_z = np.linalg.lstsq(zt, yt, rcond=None)[0]
            resid = yt - zt @ beta_z
            ssr = float(resid @ resid)
            sst = float(yt @ yt)
            ssr_null = sst

            if cluster_ids is not None:
                # Clustered F-test (sandwich estimator)
                n_clusters = len(np.unique(cluster_ids))
                bread = np.linalg.inv(zt.T @ zt)
                meat = np.zeros((k, k))
                for c in np.unique(cluster_ids):
                    mask = cluster_ids == c
                    score_c = zt[mask].T @ resid[mask]
                    meat += np.outer(score_c, score_c)
                scale = n_clusters / (n_clusters - 1) * (n - 1) / (n - k)
                vcov = bread @ (meat * scale) @ bread
                f_stat = float(beta_z @ np.linalg.inv(vcov) @ beta_z) / k
            else:
                # Standard F-test
                mse = ssr / (n - k)
                f_stat = ((ssr_null - ssr) / k) / mse if mse > 0 else 0.0

            accepted[gi] = f_stat < f_crit

        except (np.linalg.LinAlgError, ValueError):
            accepted[gi] = True

    # Extract CI bounds
    if accepted.all():
        return AndersonRubinResult(
            ci_lower=float("-inf"),
            ci_upper=float("inf"),
            alpha=alpha,
            grid_min=grid_min,
            grid_max=grid_max,
            grid_points=grid_points,
            unbounded=True,
            n_obs=n,
        )

    if not accepted.any():
        return AndersonRubinResult(
            ci_lower=float("nan"),
            ci_upper=float("nan"),
            alpha=alpha,
            grid_min=grid_min,
            grid_max=grid_max,
            grid_points=grid_points,
            unbounded=False,
            n_obs=n,
        )

    accepted_grid = grid[accepted]
    return AndersonRubinResult(
        ci_lower=float(accepted_grid.min()),
        ci_upper=float(accepted_grid.max()),
        alpha=alpha,
        grid_min=grid_min,
        grid_max=grid_max,
        grid_points=grid_points,
        unbounded=False,
        n_obs=n,
    )


def run_iv(
    df: pd.DataFrame,
    y: str,
    endog: str | list[str],
    instruments: str | list[str],
    controls: list[str] | None = None,
    fe: str | list[str] | None = None,
    cluster: str | None = None,
    alpha: float = 0.05,
) -> EstimationResult:
    """Run IV/2SLS estimation.

    Parameters
    ----------
    df : DataFrame
        Input data.
    y : str
        Dependent variable.
    endog : str or list of str
        Endogenous regressor(s) to be instrumented.
    instruments : str or list of str
        Excluded instruments (not included in the second stage directly).
    controls : list of str, optional
        Exogenous controls (included in both stages).
    fe : str or list of str, optional
        Fixed effects columns (added as dummies).
    cluster : str, optional
        Cluster variable for standard errors.
    alpha : float
        Significance level for confidence intervals.

    Returns
    -------
    EstimationResult
        Includes first-stage F-stat and overidentification diagnostics.
    """
    if isinstance(endog, str):
        endog = [endog]
    if isinstance(instruments, str):
        instruments = [instruments]
    exog_vars = list(controls) if controls else []

    # Collect all needed columns
    cols = [y] + endog + instruments + exog_vars
    if cluster:
        cols.append(cluster)

    fe_cols: list[str] = []
    if fe:
        fe_cols = [fe] if isinstance(fe, str) else list(fe)
        cols += fe_cols

    working = df[list(set(cols))].dropna().copy()

    # Build matrices
    dep = working[y]
    endog_df = working[endog]

    # Exogenous regressors (controls + constant + FE dummies)
    exog_parts = []
    if exog_vars:
        exog_parts.append(working[exog_vars])

    for col in fe_cols:
        dummies = pd.get_dummies(working[col], prefix=f"fe_{col}", drop_first=True, dtype=float)
        exog_parts.append(dummies)

    # Add constant
    const = pd.DataFrame({"const": np.ones(len(working))}, index=working.index)
    exog_parts.insert(0, const)
    exog_df = pd.concat(exog_parts, axis=1)

    instr_df = working[instruments]

    # Estimate
    model = IV2SLS(dependent=dep, exog=exog_df, endog=endog_df, instruments=instr_df)

    if cluster is not None:
        result = model.fit(cov_type="clustered", clusters=working[cluster])
    else:
        result = model.fit(cov_type="robust")

    # Variables to report (not FE dummies)
    report_vars = ["const"] + exog_vars + endog
    ci = result.conf_int(level=1.0 - alpha)

    coef_dict: dict[str, float] = {}
    se_dict: dict[str, float] = {}
    pval_dict: dict[str, float] = {}
    ci_lo: dict[str, float] = {}
    ci_hi: dict[str, float] = {}

    for var in report_vars:
        if var in result.params.index:
            coef_dict[var] = float(result.params[var])
            se_dict[var] = float(result.std_errors[var])
            pval_dict[var] = float(result.pvalues[var])
            ci_lo[var] = float(ci.loc[var, "lower"])
            ci_hi[var] = float(ci.loc[var, "upper"])

    # Diagnostics
    diag: dict = {}

    # First-stage diagnostics
    first_stage = result.first_stage
    if first_stage is not None:
        diag["first_stage"] = {}
        for e_var in endog:
            fs = first_stage.diagnostics.loc[e_var]
            diag["first_stage"][e_var] = {
                "f_stat": float(fs["f.stat"]),
                "f_pval": float(fs["f.pval"]),
                "partial_r2": float(fs["partial.rsquared"])
                if "partial.rsquared" in fs.index
                else None,
                "shea_r2": float(fs["shea.rsquared"]) if "shea.rsquared" in fs.index else None,
            }

    # Overidentification test (only if overidentified: n_instruments > n_endog)
    if len(instruments) > len(endog):
        try:
            wh = result.wooldridge_overid
            diag["overid_test"] = {
                "test": "Wooldridge overidentification",
                "stat": float(wh.stat),
                "pval": float(wh.pval),
            }
        except Exception:  # noqa: S110
            pass

    # Weak instrument: Cragg-Donald (from first stage)
    if first_stage is not None:
        for e_var in endog:
            fs_info = diag.get("first_stage", {}).get(e_var, {})
            f_val = fs_info.get("f_stat")
            if f_val is not None:
                # Stock-Yogo critical values for single endogenous regressor
                weak_flag = f_val < 10.0
                diag.setdefault("weak_instrument_flags", {})[e_var] = {
                    "f_stat": f_val,
                    "below_stock_yogo_10": weak_flag,
                }

    if cluster is not None:
        diag["n_clusters"] = int(working[cluster].nunique())
        diag["cluster_var"] = cluster

    return EstimationResult(
        coef=coef_dict,
        se=se_dict,
        pval=pval_dict,
        ci_lower=ci_lo,
        ci_upper=ci_hi,
        n_obs=int(result.nobs),
        r_sq=float(result.rsquared),
        adj_r_sq=float(result.rsquared_adj) if hasattr(result, "rsquared_adj") else None,
        method="IV-2SLS",
        depvar=y,
        diagnostics=diag,
    )
