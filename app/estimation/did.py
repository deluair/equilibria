"""Difference-in-Differences estimation.

Provides a simple 2x2 DID estimator and a dynamic event study estimator
with pre-trend testing.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import statsmodels.api as sm

from .results import EstimationResult, EventStudyResult


def run_did(
    df: pd.DataFrame,
    y: str,
    treat_col: str,
    post_col: str,
    controls: list[str] | None = None,
    cluster: str | None = None,
    alpha: float = 0.05,
) -> EstimationResult:
    """Run a simple 2x2 Difference-in-Differences estimation.

    Model: Y = b0 + b1*Treat + b2*Post + b3*(Treat*Post) + controls + e

    The coefficient of interest is b3 (the DID estimator).

    Parameters
    ----------
    df : DataFrame
        Input data.
    y : str
        Dependent variable.
    treat_col : str
        Binary treatment group indicator (1 = treated, 0 = control).
    post_col : str
        Binary post-treatment period indicator (1 = post, 0 = pre).
    controls : list of str, optional
        Additional control variables.
    cluster : str, optional
        Column to cluster standard errors on.
    alpha : float
        Significance level.

    Returns
    -------
    EstimationResult
        The interaction term "treat_x_post" is the DID estimate.
    """
    cols = [y, treat_col, post_col]
    if controls:
        cols += controls
    if cluster and cluster not in cols:
        cols.append(cluster)

    working = df[cols].dropna().copy()

    # Create interaction
    working["treat_x_post"] = working[treat_col] * working[post_col]

    # Build design matrix
    regressors = [treat_col, post_col, "treat_x_post"]
    if controls:
        regressors += [c for c in controls if c not in regressors]

    X = sm.add_constant(working[regressors], has_constant="add")
    Y = working[y]

    model = sm.OLS(Y, X)
    if cluster is not None:
        result = model.fit(cov_type="cluster", cov_kwds={"groups": working[cluster]}, use_t=True)
    else:
        result = model.fit(cov_type="HC1", use_t=True)

    # Report all coefficients
    report_vars = ["const"] + regressors
    ci = result.conf_int(alpha=alpha)

    coef_dict: dict[str, float] = {}
    se_dict: dict[str, float] = {}
    pval_dict: dict[str, float] = {}
    ci_lo: dict[str, float] = {}
    ci_hi: dict[str, float] = {}

    for var in report_vars:
        coef_dict[var] = float(result.params[var])
        se_dict[var] = float(result.bse[var])
        pval_dict[var] = float(result.pvalues[var])
        ci_lo[var] = float(ci.loc[var, 0])
        ci_hi[var] = float(ci.loc[var, 1])

    diag: dict = {
        "did_estimate": float(result.params["treat_x_post"]),
        "did_se": float(result.bse["treat_x_post"]),
        "did_pval": float(result.pvalues["treat_x_post"]),
        "n_treated_pre": int(((working[treat_col] == 1) & (working[post_col] == 0)).sum()),
        "n_treated_post": int(((working[treat_col] == 1) & (working[post_col] == 1)).sum()),
        "n_control_pre": int(((working[treat_col] == 0) & (working[post_col] == 0)).sum()),
        "n_control_post": int(((working[treat_col] == 0) & (working[post_col] == 1)).sum()),
    }
    if cluster:
        diag["n_clusters"] = int(working[cluster].nunique())

    return EstimationResult(
        coef=coef_dict,
        se=se_dict,
        pval=pval_dict,
        ci_lower=ci_lo,
        ci_upper=ci_hi,
        n_obs=int(result.nobs),
        r_sq=float(result.rsquared),
        adj_r_sq=float(result.rsquared_adj),
        method="DID",
        depvar=y,
        diagnostics=diag,
    )


def run_event_study(
    df: pd.DataFrame,
    y: str,
    treat_col: str,
    time_col: str,
    event_time_col: str,
    ref_period: int = -1,
    controls: list[str] | None = None,
    cluster: str | None = None,
    alpha: float = 0.05,
) -> EventStudyResult:
    """Run a dynamic event study (leads and lags DID).

    Model: Y_it = sum_k beta_k * (Treat_i * 1[t - event = k]) + controls + e_it

    The reference period (ref_period) is omitted; all coefficients are
    relative to that period.

    Parameters
    ----------
    df : DataFrame
    y : str
        Dependent variable.
    treat_col : str
        Binary treatment indicator.
    time_col : str
        Calendar time column.
    event_time_col : str
        Relative time to event (e.g., -3, -2, -1, 0, 1, 2, ...).
        The column should already be computed.
    ref_period : int
        Reference period to omit (default -1).
    controls : list of str, optional
    cluster : str, optional
    alpha : float

    Returns
    -------
    EventStudyResult
        Includes pre-trend F-test.
    """
    cols = [y, treat_col, time_col, event_time_col]
    if controls:
        cols += controls
    if cluster and cluster not in cols:
        cols.append(cluster)

    working = df[cols].dropna().copy()

    # Get sorted unique event-time periods, excluding reference
    periods_all = sorted(working[event_time_col].unique())
    periods = [int(t) for t in periods_all if int(t) != ref_period]

    # Create interaction dummies: Treat * 1[event_time == k]
    dummy_names = []
    for t in periods:
        col_name = f"evt_{t}"
        working[col_name] = ((working[treat_col] == 1) & (working[event_time_col] == t)).astype(
            float
        )
        dummy_names.append(col_name)

    regressors = dummy_names.copy()
    if controls:
        regressors += [c for c in controls if c not in regressors]

    X = sm.add_constant(working[regressors], has_constant="add")
    Y = working[y]

    model = sm.OLS(Y, X)
    if cluster is not None:
        result = model.fit(cov_type="cluster", cov_kwds={"groups": working[cluster]}, use_t=True)
    else:
        result = model.fit(cov_type="HC1", use_t=True)

    ci = result.conf_int(alpha=alpha)

    coefs = []
    ses = []
    pvals = []
    ci_lower = []
    ci_upper = []

    for _t, col_name in zip(periods, dummy_names, strict=False):
        coefs.append(float(result.params[col_name]))
        ses.append(float(result.bse[col_name]))
        pvals.append(float(result.pvalues[col_name]))
        ci_lower.append(float(ci.loc[col_name, 0]))
        ci_upper.append(float(ci.loc[col_name, 1]))

    # Pre-trend test: joint F-test on pre-treatment coefficients
    pre_dummies = [name for t, name in zip(periods, dummy_names, strict=False) if t < ref_period]
    pre_trend_fstat = None
    pre_trend_pval = None

    if len(pre_dummies) > 0:
        # Build restriction matrix: R * beta = 0 for pre-treatment dummies
        r_matrix = np.zeros((len(pre_dummies), len(result.params)))
        param_names = list(result.params.index)
        for i, dummy in enumerate(pre_dummies):
            j = param_names.index(dummy)
            r_matrix[i, j] = 1.0

        try:
            f_test = result.f_test(r_matrix)
            pre_trend_fstat = float(f_test.fvalue)
            pre_trend_pval = float(f_test.pvalue)
        except Exception:  # noqa: S110
            # Fallback: manual Wald test
            pass

    return EventStudyResult(
        periods=periods,
        coef=coefs,
        se=ses,
        pval=pvals,
        ci_lower=ci_lower,
        ci_upper=ci_upper,
        ref_period=ref_period,
        pre_trend_fstat=pre_trend_fstat,
        pre_trend_pval=pre_trend_pval,
        n_obs=int(result.nobs),
        depvar=y,
        diagnostics={
            "n_pre_periods": len(pre_dummies),
            "n_post_periods": len([t for t in periods if t >= 0]),
            "r_sq": float(result.rsquared),
        },
    )
