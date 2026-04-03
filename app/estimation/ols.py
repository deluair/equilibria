"""OLS estimation wrapper around statsmodels.

Supports heteroskedasticity-robust and clustered standard errors,
optional fixed effects via dummies (for small FE dimensions) or
demeaning via pyfixest (for high-dimensional FE).
"""

from __future__ import annotations

import pandas as pd
import statsmodels.api as sm
from scipy import stats

from .results import EstimationResult


def run_ols(
    df: pd.DataFrame,
    y: str,
    x: str | list[str],
    controls: list[str] | None = None,
    fe: str | list[str] | None = None,
    cluster: str | None = None,
    robust: bool = True,
    alpha: float = 0.05,
) -> EstimationResult:
    """Run OLS regression with optional robust/clustered SEs and fixed effects.

    Parameters
    ----------
    df : DataFrame
        Input data. Rows with NaN in any relevant column are dropped.
    y : str
        Name of the dependent variable column.
    x : str or list of str
        Treatment / main independent variable(s).
    controls : list of str, optional
        Additional control variable columns.
    fe : str or list of str, optional
        Column(s) to include as fixed effects (dummy variables).
        For high-dimensional FE (> 50 categories), consider panel_fe instead.
    cluster : str, optional
        Column to cluster standard errors on. Overrides robust if provided.
    robust : bool
        If True and cluster is None, use HC1 robust standard errors.
    alpha : float
        Significance level for confidence intervals (default 0.05).

    Returns
    -------
    EstimationResult
        Standardized result object.
    """
    if isinstance(x, str):
        x = [x]
    regressors = list(x)
    if controls:
        regressors += [c for c in controls if c not in regressors]

    # Columns needed (before FE expansion)
    cols_needed = [y] + regressors
    if cluster:
        cols_needed.append(cluster)
    if fe:
        fe_cols = [fe] if isinstance(fe, str) else list(fe)
        cols_needed += fe_cols
    else:
        fe_cols = []

    working = df[cols_needed].dropna().copy()

    # Build design matrix
    X = working[regressors].copy()

    # Add fixed effects as dummies (drop first to avoid collinearity)
    fe_names: list[str] = []
    for col in fe_cols:
        dummies = pd.get_dummies(working[col], prefix=f"fe_{col}", drop_first=True, dtype=float)
        fe_names += list(dummies.columns)
        X = pd.concat([X, dummies], axis=1)

    X = sm.add_constant(X, has_constant="add")
    Y = working[y]

    model = sm.OLS(Y, X)

    # Fit with appropriate covariance
    if cluster is not None:
        groups = working[cluster]
        result = model.fit(cov_type="cluster", cov_kwds={"groups": groups}, use_t=True)
    elif robust:
        result = model.fit(cov_type="HC1", use_t=True)
    else:
        result = model.fit(use_t=True)

    # Extract results for main regressors only (not FE dummies, not const)
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

    # Diagnostics
    diag: dict = {
        "f_stat": float(result.fvalue) if hasattr(result, "fvalue") and result.fvalue else None,
        "f_pval": float(result.f_pvalue)
        if hasattr(result, "f_pvalue") and result.f_pvalue
        else None,
        "aic": float(result.aic),
        "bic": float(result.bic),
    }
    if cluster is not None:
        diag["n_clusters"] = int(working[cluster].nunique())
        diag["cluster_var"] = cluster
    if fe_cols:
        diag["fe_vars"] = fe_cols
        diag["n_fe_dummies"] = len(fe_names)
    diag["robust"] = robust or (cluster is not None)

    return EstimationResult(
        coef=coef_dict,
        se=se_dict,
        pval=pval_dict,
        ci_lower=ci_lo,
        ci_upper=ci_hi,
        n_obs=int(result.nobs),
        r_sq=float(result.rsquared),
        adj_r_sq=float(result.rsquared_adj),
        method="OLS",
        depvar=y,
        diagnostics=diag,
    )


def _partial_ftest(
    result_unrestricted: sm.regression.linear_model.RegressionResultsWrapper,
    result_restricted: sm.regression.linear_model.RegressionResultsWrapper,
    q: int,
) -> tuple[float, float]:
    """Compute a partial F-test between restricted and unrestricted models.

    Parameters
    ----------
    result_unrestricted : statsmodels result
    result_restricted : statsmodels result
    q : int
        Number of restrictions (difference in parameters).

    Returns
    -------
    (f_stat, p_value)
    """
    ssr_r = result_restricted.ssr
    ssr_u = result_unrestricted.ssr
    n = result_unrestricted.nobs
    k = result_unrestricted.df_model + 1
    f_stat = ((ssr_r - ssr_u) / q) / (ssr_u / (n - k))
    p_val = 1.0 - stats.f.cdf(f_stat, q, n - k)
    return float(f_stat), float(p_val)
