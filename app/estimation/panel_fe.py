"""Panel fixed effects estimation using linearmodels PanelOLS.

Supports entity and time fixed effects, clustered standard errors,
and a Hausman test comparing fixed vs. random effects.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from linearmodels.panel import PanelOLS, RandomEffects
from scipy import stats

from .results import EstimationResult


def run_panel_fe(
    df: pd.DataFrame,
    y: str,
    x: str | list[str],
    entity_col: str,
    time_col: str,
    entity_fe: bool = True,
    time_fe: bool = True,
    controls: list[str] | None = None,
    cluster: str | None = None,
    alpha: float = 0.05,
) -> EstimationResult:
    """Run panel fixed effects regression.

    Parameters
    ----------
    df : DataFrame
        Panel data in long format.
    y : str
        Dependent variable.
    x : str or list of str
        Main independent variable(s).
    entity_col : str
        Column identifying panel entities (e.g. country, firm).
    time_col : str
        Column identifying time periods.
    entity_fe : bool
        Include entity fixed effects (default True).
    time_fe : bool
        Include time fixed effects (default True).
    controls : list of str, optional
        Additional control variables.
    cluster : str, optional
        Cluster variable. Defaults to entity_col if entity_fe is True.
    alpha : float
        Significance level for confidence intervals.

    Returns
    -------
    EstimationResult
        Includes within R-squared in diagnostics.
    """
    if isinstance(x, str):
        x = [x]
    regressors = list(x)
    if controls:
        regressors += [c for c in controls if c not in regressors]

    cols = [y, entity_col, time_col] + regressors
    if cluster and cluster not in cols:
        cols.append(cluster)

    working = df[cols].dropna().copy()

    # Set multi-index for panel structure
    working = working.set_index([entity_col, time_col])

    dep = working[y]
    exog = working[regressors]

    model = PanelOLS(
        dependent=dep,
        exog=exog,
        entity_effects=entity_fe,
        time_effects=time_fe,
        check_rank=False,
    )

    # Determine clustering
    if cluster is not None:
        if cluster in (entity_col, time_col):
            # linearmodels supports EntityEffects clustering directly
            cov_type = "clustered"
            cov_kwds = {
                "cluster_entity": cluster == entity_col,
                "cluster_time": cluster == time_col,
            }
        else:
            cov_kwds = {"clusters": working[cluster]}
            cov_type = "clustered"
    else:
        # Default: cluster on entity if entity FE
        cov_type = "clustered"
        cov_kwds = {"cluster_entity": entity_fe}

    result = model.fit(cov_type=cov_type, **cov_kwds)

    # Build output
    ci = result.conf_int(level=1.0 - alpha)
    report_vars = regressors

    coef_dict: dict[str, float] = {}
    se_dict: dict[str, float] = {}
    pval_dict: dict[str, float] = {}
    ci_lo: dict[str, float] = {}
    ci_hi: dict[str, float] = {}

    for var in report_vars:
        coef_dict[var] = float(result.params[var])
        se_dict[var] = float(result.std_errors[var])
        pval_dict[var] = float(result.pvalues[var])
        ci_lo[var] = float(ci.loc[var, "lower"])
        ci_hi[var] = float(ci.loc[var, "upper"])

    diag: dict = {
        "within_r_sq": float(result.rsquared_within),
        "between_r_sq": float(result.rsquared_between)
        if hasattr(result, "rsquared_between")
        else None,
        "overall_r_sq": float(result.rsquared_overall)
        if hasattr(result, "rsquared_overall")
        else None,
        "entity_fe": entity_fe,
        "time_fe": time_fe,
        "f_stat": float(result.f_statistic.stat),
        "f_pval": float(result.f_statistic.pval),
        "n_entities": int(result.entity_info["total"]),
    }
    if cluster:
        diag["cluster_var"] = cluster

    return EstimationResult(
        coef=coef_dict,
        se=se_dict,
        pval=pval_dict,
        ci_lower=ci_lo,
        ci_upper=ci_hi,
        n_obs=int(result.nobs),
        r_sq=float(result.rsquared),
        adj_r_sq=None,
        method="Panel FE",
        depvar=y,
        diagnostics=diag,
    )


def hausman_test(
    df: pd.DataFrame,
    y: str,
    x: str | list[str],
    entity_col: str,
    time_col: str,
    controls: list[str] | None = None,
) -> dict:
    """Run a Hausman test comparing FE vs RE estimators.

    The null hypothesis is that RE is consistent and efficient.
    Rejection implies the FE model should be preferred.

    Parameters
    ----------
    df : DataFrame
    y : str
    x : str or list of str
    entity_col : str
    time_col : str
    controls : list of str, optional

    Returns
    -------
    dict with keys: chi2, df, pval, prefer ("FE" or "RE at alpha=0.05")
    """
    if isinstance(x, str):
        x = [x]
    regressors = list(x)
    if controls:
        regressors += [c for c in controls if c not in regressors]

    cols = [y, entity_col, time_col] + regressors
    working = df[cols].dropna().set_index([entity_col, time_col])

    dep = working[y]
    exog = working[regressors]

    # Fixed effects
    fe_model = PanelOLS(dependent=dep, exog=exog, entity_effects=True)
    fe_result = fe_model.fit()

    # Random effects
    re_model = RandomEffects(dependent=dep, exog=exog)
    re_result = re_model.fit()

    # Hausman test statistic
    b_fe = fe_result.params[regressors].values
    b_re = re_result.params[regressors].values
    diff = b_fe - b_re

    cov_fe = fe_result.cov[regressors].loc[regressors].values
    cov_re = re_result.cov[regressors].loc[regressors].values
    cov_diff = cov_fe - cov_re

    # Use pseudo-inverse for numerical stability
    try:
        chi2_stat = float(diff @ np.linalg.inv(cov_diff) @ diff)
    except np.linalg.LinAlgError:
        chi2_stat = float(diff @ np.linalg.pinv(cov_diff) @ diff)

    k = len(regressors)
    p_val = 1.0 - stats.chi2.cdf(chi2_stat, k)

    return {
        "chi2": chi2_stat,
        "df": k,
        "pval": p_val,
        "prefer": "FE" if p_val < 0.05 else "RE at alpha=0.05",
    }
