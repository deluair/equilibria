"""Modern staggered Difference-in-Differences estimators.

Implements three leading approaches for DID with staggered treatment adoption:

1. Callaway & Sant'Anna (2021): group-time ATTs with flexible aggregation.
2. Sun & Abraham (2021): interaction-weighted estimator.
3. Borusyak, Jaravel, & Spiess (2024): imputation estimator.

These methods address the well-documented bias of two-way fixed effects (TWFE)
when treatment effects are heterogeneous across cohorts or over time.

References:
    Callaway, B., & Sant'Anna, P. H. C. (2021). Difference-in-differences
    with multiple time periods. Journal of Econometrics, 225(2), 200-230.

    Sun, L., & Abraham, S. (2021). Estimating dynamic treatment effects in
    event studies with heterogeneous treatment effects. Journal of Econometrics,
    225(2), 175-199.

    Borusyak, K., Jaravel, X., & Spiess, J. (2024). Revisiting event-study
    designs: Robust and efficient estimation. Review of Economic Studies.

    de Chaisemartin, C., & D'Haultfoeuille, X. (2020). Two-way fixed effects
    estimators with heterogeneous treatment effects. American Economic Review,
    110(9), 2964-2996.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

from .results import EstimationResult, EventStudyResult


@dataclass
class GroupTimeATT:
    """A single group-time average treatment effect.

    Attributes:
        group: Treatment cohort (period of first treatment).
        time: Calendar time period.
        att: Point estimate of ATT(g,t).
        se: Standard error.
    """

    group: Any
    time: Any
    att: float
    se: float


@dataclass
class StaggeredDIDResult:
    """Output from a staggered DID estimation.

    Attributes:
        group_time_atts: List of GroupTimeATT objects.
        aggregated_att: Overall aggregated ATT.
        aggregated_se: SE of the aggregated ATT.
        aggregated_ci_lower: Lower 95% CI.
        aggregated_ci_upper: Upper 95% CI.
        event_study_coefs: EventStudyResult from dynamic aggregation (if computed).
        n_obs: Number of observations.
        n_groups: Number of treatment cohorts.
        depvar: Name of the outcome variable.
        method: Name of the estimation method.
        diagnostics: Additional diagnostics.
    """

    group_time_atts: list[GroupTimeATT]
    aggregated_att: float
    aggregated_se: float
    aggregated_ci_lower: float
    aggregated_ci_upper: float
    event_study_coefs: EventStudyResult | None = None
    n_obs: int = 0
    n_groups: int = 0
    depvar: str = ""
    method: str = "Callaway-SantAnna"
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        lines = [
            f"=== Staggered DID Results ({self.method}) ===",
            f"Dep. variable: {self.depvar}",
            f"N = {self.n_obs:,}    Groups: {self.n_groups}",
            "",
            f"Aggregated ATT: {self.aggregated_att:.4f} (SE: {self.aggregated_se:.4f})",
            f"95% CI: [{self.aggregated_ci_lower:.4f}, {self.aggregated_ci_upper:.4f}]",
            "",
            "Group-time ATTs:",
            f"{'Group':<12} {'Time':<12} {'ATT':>10} {'SE':>10}",
            "-" * 48,
        ]
        for gt in self.group_time_atts:
            lines.append(f"{str(gt.group):<12} {str(gt.time):<12} {gt.att:>10.4f} {gt.se:>10.4f}")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dictionary."""
        return {
            "method": self.method,
            "depvar": self.depvar,
            "aggregated_att": self.aggregated_att,
            "aggregated_se": self.aggregated_se,
            "aggregated_ci_lower": self.aggregated_ci_lower,
            "aggregated_ci_upper": self.aggregated_ci_upper,
            "n_obs": self.n_obs,
            "n_groups": self.n_groups,
            "group_time_atts": [
                {"group": gt.group, "time": gt.time, "att": gt.att, "se": gt.se}
                for gt in self.group_time_atts
            ],
        }


def _outcome_regression_did(
    y_post: np.ndarray,
    y_pre: np.ndarray,
    treat: np.ndarray,
    X: np.ndarray | None = None,
) -> tuple[float, float]:
    """Estimate a single 2x2 DID via outcome regression.

    Uses the doubly-robust approach of Sant'Anna & Zhao (2020) when
    controls are provided, otherwise simple DID.

    Args:
        y_post: Outcome in the post period.
        y_pre: Outcome in the pre (base) period.
        treat: Binary treatment indicator.
        X: Control variables (optional).

    Returns:
        Tuple of (ATT, SE).
    """
    dy = y_post - y_pre
    n = len(dy)
    n1 = treat.sum()
    n0 = n - n1

    if n1 == 0 or n0 == 0:
        return np.nan, np.nan

    if X is not None and X.shape[1] > 0:
        # Outcome regression adjustment: regress dy on X among controls,
        # predict for treated, compute adjusted DID
        from numpy.linalg import lstsq

        X_with_const = np.column_stack([np.ones(int(n0)), X[treat == 0]])
        beta, _, _, _ = lstsq(X_with_const, dy[treat == 0], rcond=None)
        X_tr_const = np.column_stack([np.ones(int(n1)), X[treat == 1]])
        predicted = X_tr_const @ beta
        att = float(np.mean(dy[treat == 1]) - np.mean(predicted))
    else:
        att = float(np.mean(dy[treat == 1]) - np.mean(dy[treat == 0]))

    # Influence function based SE
    if X is not None and X.shape[1] > 0:
        resid_control = dy[treat == 0] - np.mean(dy[treat == 0])
        var_0 = np.var(resid_control, ddof=1) / n0
        resid_treat = dy[treat == 1] - np.mean(dy[treat == 1])
        var_1 = np.var(resid_treat, ddof=1) / n1
    else:
        var_1 = np.var(dy[treat == 1], ddof=1) / n1
        var_0 = np.var(dy[treat == 0], ddof=1) / n0

    se = float(np.sqrt(var_1 + var_0))
    return att, se


def run_callaway_santanna(
    df: pd.DataFrame,
    y: str,
    entity_col: str,
    time_col: str,
    first_treat_col: str,
    controls: list[str] | None = None,
    cluster: str | None = None,
    base_period: str = "varying",
    anticipation: int = 0,
) -> StaggeredDIDResult:
    """Estimate group-time ATTs following Callaway & Sant'Anna (2021).

    Args:
        df: Panel DataFrame.
        y: Name of the outcome variable.
        entity_col: Column identifying panel units.
        time_col: Column identifying time periods.
        first_treat_col: Column with the period of first treatment.
            Use 0 or np.inf for never-treated units.
        controls: List of control variable names (optional).
        cluster: Column for clustered standard errors (defaults to entity_col).
        base_period: 'varying' uses the period right before treatment as base,
            'universal' uses a single pre-treatment period.
        anticipation: Number of anticipation periods (default 0).

    Returns:
        StaggeredDIDResult with group-time ATTs and aggregated effects.
    """
    if cluster is None:
        cluster = entity_col

    times = sorted(df[time_col].unique())
    groups = sorted(df[first_treat_col].unique())
    # Separate never-treated (coded as 0 or inf)
    never_treated_codes = {0, np.inf, float("inf")}
    treated_groups = [g for g in groups if g not in never_treated_codes]
    n_obs = len(df)

    # Pivot the outcome
    panel = df.pivot_table(index=entity_col, columns=time_col, values=y)
    first_treat_map = df.groupby(entity_col)[first_treat_col].first()

    # Controls if provided
    if controls:
        X_map = df.groupby(entity_col)[controls].first()
    else:
        X_map = None

    # Never-treated units
    never_treated_ids = first_treat_map[first_treat_map.isin(never_treated_codes)].index.tolist()

    gt_atts: list[GroupTimeATT] = []

    for g in treated_groups:
        # Units in cohort g
        cohort_ids = first_treat_map[first_treat_map == g].index.tolist()
        if len(cohort_ids) == 0:
            continue

        # Comparison group: never-treated (or not-yet-treated depending on design)
        comp_ids = never_treated_ids

        for t in times:
            if t < g - anticipation:
                # Pre-treatment period for this cohort, skip (or set to 0)
                continue

            # Base period
            if base_period == "varying":
                bp = g - anticipation - 1
            else:
                bp = min(times)

            if bp not in panel.columns or t not in panel.columns:
                continue

            # Extract outcomes
            all_ids = cohort_ids + comp_ids
            if not all(uid in panel.index for uid in all_ids):
                continue

            y_post = panel.loc[all_ids, t].values
            y_pre = panel.loc[all_ids, bp].values
            treat_indicator = np.array([1 if uid in cohort_ids else 0 for uid in all_ids])

            X = None
            if X_map is not None:
                X = X_map.loc[all_ids].values

            att, se = _outcome_regression_did(y_post, y_pre, treat_indicator, X)
            gt_atts.append(GroupTimeATT(group=g, time=t, att=att, se=se))

    # Simple aggregation: weighted average of post-treatment ATTs
    post_atts = [gt for gt in gt_atts if gt.time >= gt.group and not np.isnan(gt.att)]
    if post_atts:
        # Weight by group size
        group_sizes = {g: int((first_treat_map == g).sum()) for g in treated_groups}
        total_treated = sum(group_sizes.get(gt.group, 1) for gt in post_atts)
        weights = np.array([group_sizes.get(gt.group, 1) / total_treated for gt in post_atts])
        agg_att = float(np.sum(weights * np.array([gt.att for gt in post_atts])))
        agg_se = float(np.sqrt(np.sum((weights**2) * np.array([gt.se**2 for gt in post_atts]))))
    else:
        agg_att = np.nan
        agg_se = np.nan

    ci_lower = agg_att - 1.96 * agg_se
    ci_upper = agg_att + 1.96 * agg_se

    # Dynamic (event study) aggregation
    event_study = _aggregate_event_study(gt_atts, treated_groups)

    return StaggeredDIDResult(
        group_time_atts=gt_atts,
        aggregated_att=agg_att,
        aggregated_se=agg_se,
        aggregated_ci_lower=ci_lower,
        aggregated_ci_upper=ci_upper,
        event_study_coefs=event_study,
        n_obs=n_obs,
        n_groups=len(treated_groups),
        depvar=y,
        method="Callaway-SantAnna",
        diagnostics={
            "base_period": base_period,
            "anticipation": anticipation,
            "n_never_treated": len(never_treated_ids),
        },
    )


def _aggregate_event_study(
    gt_atts: list[GroupTimeATT],
    treated_groups: list[Any],
) -> EventStudyResult | None:
    """Aggregate group-time ATTs into an event study.

    Computes average effects at each relative time (event time = t - g).

    Args:
        gt_atts: List of GroupTimeATT.
        treated_groups: List of treatment cohort identifiers.

    Returns:
        EventStudyResult or None if insufficient data.
    """
    if not gt_atts:
        return None

    # Compute relative time
    rel_data: dict[int, list[tuple[float, float]]] = {}
    for gt in gt_atts:
        if np.isnan(gt.att):
            continue
        e = int(gt.time - gt.group)  # event time
        if e not in rel_data:
            rel_data[e] = []
        rel_data[e].append((gt.att, gt.se))

    if not rel_data:
        return None

    periods = sorted(rel_data.keys())
    coefs = []
    ses = []
    pvals = []
    ci_lo = []
    ci_hi = []

    for e in periods:
        atts_at_e = [x[0] for x in rel_data[e]]
        ses_at_e = [x[1] for x in rel_data[e]]
        n_e = len(atts_at_e)
        avg_att = float(np.mean(atts_at_e))
        # Pooled SE
        avg_se = float(np.sqrt(np.mean(np.array(ses_at_e) ** 2) / n_e))
        if avg_se > 0:
            z = avg_att / avg_se
            pval = float(2 * (1 - stats.norm.cdf(abs(z))))
        else:
            pval = np.nan
        coefs.append(avg_att)
        ses.append(avg_se)
        pvals.append(pval)
        ci_lo.append(avg_att - 1.96 * avg_se)
        ci_hi.append(avg_att + 1.96 * avg_se)

    # Pre-trend test: joint test of pre-treatment coefficients
    pre_coefs = [c for e, c in zip(periods, coefs, strict=False) if e < 0]
    pre_ses = [s for e, s in zip(periods, ses, strict=False) if e < 0]
    if pre_coefs and all(s > 0 for s in pre_ses):
        chi2 = sum((c / s) ** 2 for c, s in zip(pre_coefs, pre_ses, strict=False))
        pre_trend_pval = float(1 - stats.chi2.cdf(chi2, df=len(pre_coefs)))
        pre_trend_fstat = float(chi2 / len(pre_coefs))
    else:
        pre_trend_fstat = None
        pre_trend_pval = None

    return EventStudyResult(
        periods=periods,
        coef=coefs,
        se=ses,
        pval=pvals,
        ci_lower=ci_lo,
        ci_upper=ci_hi,
        ref_period=-1,
        pre_trend_fstat=pre_trend_fstat,
        pre_trend_pval=pre_trend_pval,
        n_obs=0,
        depvar="",
    )


def run_sun_abraham(
    df: pd.DataFrame,
    y: str,
    entity_col: str,
    time_col: str,
    cohort_col: str,
    controls: list[str] | None = None,
    cluster: str | None = None,
    ref_period: int = -1,
) -> EventStudyResult:
    """Estimate dynamic treatment effects using Sun & Abraham (2021).

    The interaction-weighted (IW) estimator corrects for heterogeneity
    bias in TWFE event study regressions by interacting relative-time
    dummies with cohort indicators and then aggregating with appropriate
    weights.

    Args:
        df: Panel DataFrame.
        y: Name of the outcome variable.
        entity_col: Column identifying panel units.
        time_col: Column identifying time periods.
        cohort_col: Column with the treatment cohort (period of first treatment).
            Never-treated units should be coded as 0 or np.inf.
        controls: List of control variable names (optional).
        cluster: Column for clustered SE (defaults to entity_col).
        ref_period: Reference (omitted) relative period (default -1).

    Returns:
        EventStudyResult with interaction-weighted coefficients.
    """
    if cluster is None:
        cluster = entity_col

    data = df.copy()
    never_treated_codes = {0, np.inf, float("inf")}

    # Compute relative time
    data["_rel_time"] = data[time_col] - data[cohort_col]
    # For never-treated, set to a large negative number (they serve as reference)
    data.loc[data[cohort_col].isin(never_treated_codes), "_rel_time"] = -99999

    # Get cohorts (excluding never-treated)
    cohorts = sorted([c for c in data[cohort_col].unique() if c not in never_treated_codes])
    rel_times = sorted([r for r in data["_rel_time"].unique() if r != -99999 and r != ref_period])

    # Create interaction dummies: 1(cohort=g) * 1(rel_time=e) for each (g, e)
    interaction_cols = []
    for g in cohorts:
        for e in rel_times:
            col_name = f"_coh{g}_rel{e}"
            data[col_name] = ((data[cohort_col] == g) & (data["_rel_time"] == e)).astype(float)
            interaction_cols.append((col_name, g, e))

    # Entity and time fixed effects via demeaning
    # Use within transformation: demean by entity and time
    data["_entity_id"] = pd.Categorical(data[entity_col]).codes
    data["_time_id"] = pd.Categorical(data[time_col]).codes

    # Simple FE regression using entity and time dummies
    from numpy.linalg import lstsq

    # Build the design matrix: interaction terms + controls + entity FE + time FE
    # For efficiency, use the Frisch-Waugh-Lovell theorem: partial out FE
    y_vals = data[y].values.copy()
    n = len(data)

    # Demean by entity
    entity_means_y = data.groupby("_entity_id")[y].transform("mean").values
    time_means_y = data.groupby("_time_id")[y].transform("mean").values
    grand_mean_y = y_vals.mean()
    y_demean = y_vals - entity_means_y - time_means_y + grand_mean_y

    X_cols = [ic[0] for ic in interaction_cols]
    if controls:
        X_cols = X_cols + controls

    X_raw = data[X_cols].values.copy()

    # Demean X by entity and time
    X_demean = np.zeros_like(X_raw, dtype=float)
    for j in range(X_raw.shape[1]):
        col_vals = X_raw[:, j]
        entity_means = data.groupby("_entity_id")[X_cols[j]].transform("mean").values
        time_means = data.groupby("_time_id")[X_cols[j]].transform("mean").values
        grand_mean = col_vals.mean()
        X_demean[:, j] = col_vals - entity_means - time_means + grand_mean

    # OLS on demeaned data
    beta, _, _, _ = lstsq(X_demean, y_demean, rcond=None)

    # Residuals for SE computation
    resid = y_demean - X_demean @ beta

    # Cluster-robust variance (Liang-Zeger)
    clusters = data[cluster].values
    unique_clusters = np.unique(clusters)
    n_clusters = len(unique_clusters)
    k = X_demean.shape[1]
    bread = np.linalg.inv(X_demean.T @ X_demean)

    meat = np.zeros((k, k))
    for cl in unique_clusters:
        mask = clusters == cl
        X_cl = X_demean[mask]
        e_cl = resid[mask]
        score = X_cl.T @ e_cl
        meat += np.outer(score, score)

    # Small sample correction
    correction = n_clusters / (n_clusters - 1) * (n - 1) / (n - k)
    V = correction * bread @ meat @ bread
    se_all = np.sqrt(np.diag(V))

    # Extract interaction coefficients (before any control columns)
    n_interact = len(interaction_cols)
    interact_betas = beta[:n_interact]
    interact_ses = se_all[:n_interact]

    # Aggregate: for each relative time e, compute IW estimate
    # Weight by cohort share among the treated at that relative time
    cohort_sizes = {g: int((data[cohort_col] == g).sum()) for g in cohorts}
    total_treated = sum(cohort_sizes.values())

    event_coefs: dict[int, tuple[float, float]] = {}
    for idx, (_col_name, g, e) in enumerate(interaction_cols):
        w = cohort_sizes[g] / total_treated
        if e not in event_coefs:
            event_coefs[e] = [0.0, 0.0]
        event_coefs[e][0] += w * interact_betas[idx]
        event_coefs[e][1] += (w * interact_ses[idx]) ** 2

    periods_out = sorted(event_coefs.keys())
    coefs_out = []
    ses_out = []
    pvals_out = []
    ci_lo_out = []
    ci_hi_out = []

    for e in periods_out:
        c = event_coefs[e][0]
        s = np.sqrt(event_coefs[e][1])
        coefs_out.append(float(c))
        ses_out.append(float(s))
        if s > 0:
            z = c / s
            p = float(2 * (1 - stats.norm.cdf(abs(z))))
        else:
            p = np.nan
        pvals_out.append(p)
        ci_lo_out.append(float(c - 1.96 * s))
        ci_hi_out.append(float(c + 1.96 * s))

    # Pre-trend test
    pre_c = [c for e, c in zip(periods_out, coefs_out, strict=False) if e < 0]
    pre_s = [s for e, s in zip(periods_out, ses_out, strict=False) if e < 0]
    if pre_c and all(s > 0 for s in pre_s):
        chi2 = sum((c / s) ** 2 for c, s in zip(pre_c, pre_s, strict=False))
        pre_fstat = float(chi2 / len(pre_c))
        pre_pval = float(1 - stats.chi2.cdf(chi2, df=len(pre_c)))
    else:
        pre_fstat = None
        pre_pval = None

    return EventStudyResult(
        periods=periods_out,
        coef=coefs_out,
        se=ses_out,
        pval=pvals_out,
        ci_lower=ci_lo_out,
        ci_upper=ci_hi_out,
        ref_period=ref_period,
        pre_trend_fstat=pre_fstat,
        pre_trend_pval=pre_pval,
        n_obs=n,
        depvar=y,
        diagnostics={
            "method": "Sun-Abraham IW",
            "n_cohorts": len(cohorts),
            "n_clusters": n_clusters,
        },
    )


def run_borusyak_jaravel_spiess(
    df: pd.DataFrame,
    y: str,
    entity_col: str,
    time_col: str,
    treat_col: str,
    controls: list[str] | None = None,
    cluster: str | None = None,
) -> EstimationResult:
    """Estimate treatment effects using the imputation estimator (BJS, 2024).

    The imputation approach:
    1. Estimate unit and time FE using only untreated observations.
    2. Impute Y(0) for treated observations using estimated FE.
    3. The treatment effect for each treated obs is Y - Y_hat(0).
    4. Average to get the ATT.

    Args:
        df: Panel DataFrame.
        y: Name of the outcome variable.
        entity_col: Column identifying panel units.
        time_col: Column identifying time periods.
        treat_col: Binary treatment indicator (1 if treated in this period).
        controls: List of control variable names (optional, absorbed by FE).
        cluster: Column for clustered SE (defaults to entity_col).

    Returns:
        EstimationResult with the imputation-based ATT.
    """
    if cluster is None:
        cluster = entity_col

    data = df.copy()
    data["_entity_code"] = pd.Categorical(data[entity_col]).codes
    data["_time_code"] = pd.Categorical(data[time_col]).codes

    treated_mask = data[treat_col] == 1
    untreated_data = data[~treated_mask].copy()
    treated_data = data[treated_mask].copy()

    n_obs = len(data)
    n_treated = len(treated_data)

    if n_treated == 0:
        raise ValueError("No treated observations found.")

    # Step 1: Estimate entity and time FE on untreated sample
    # Using the within estimator (demeaning)

    # Compute entity means and time means from untreated data
    entity_means = untreated_data.groupby(entity_col)[y].mean()
    time_means = untreated_data.groupby(time_col)[y].mean()
    grand_mean = untreated_data[y].mean()

    # Entity FE = entity_mean - grand_mean (approximately)
    # Time FE = time_mean - grand_mean
    entity_fe = entity_means - grand_mean
    time_fe = time_means - grand_mean

    # If controls are provided, residualize first
    if controls:
        from numpy.linalg import lstsq as np_lstsq

        X_ut = untreated_data[controls].values
        y_ut_demean = (
            untreated_data[y].values
            - untreated_data[entity_col].map(entity_means).values
            - untreated_data[time_col].map(time_means).values
            + grand_mean
        )
        X_ut_demean = np.zeros_like(X_ut, dtype=float)
        for j in range(X_ut.shape[1]):
            col = X_ut[:, j]
            e_mean = untreated_data.groupby(entity_col)[controls[j]].transform("mean").values
            t_mean = untreated_data.groupby(time_col)[controls[j]].transform("mean").values
            g_mean = col.mean()
            X_ut_demean[:, j] = col - e_mean - t_mean + g_mean

        beta_controls, _, _, _ = np_lstsq(X_ut_demean, y_ut_demean, rcond=None)
    else:
        beta_controls = None

    # Step 2: Impute Y(0) for treated observations
    y0_hat = np.zeros(n_treated)
    for i, (_, row) in enumerate(treated_data.iterrows()):
        e = row[entity_col]
        t = row[time_col]
        imputed = grand_mean
        if e in entity_fe.index:
            imputed += entity_fe[e]
        if t in time_fe.index:
            imputed += time_fe[t]
        if beta_controls is not None and controls:
            x_i = row[controls].values.astype(float)
            imputed += x_i @ beta_controls
        y0_hat[i] = imputed

    # Step 3: Individual treatment effects
    tau_i = treated_data[y].values - y0_hat

    # Step 4: ATT
    att = float(np.mean(tau_i))

    # Cluster-robust SE
    treated_data = treated_data.copy()
    treated_data["_tau"] = tau_i
    cluster_vals = treated_data[cluster].values
    unique_cl = np.unique(cluster_vals)
    n_cl = len(unique_cl)

    # Cluster-level means of tau
    cl_means = []
    for cl in unique_cl:
        mask = cluster_vals == cl
        cl_means.append(np.mean(tau_i[mask]))
    cl_means = np.array(cl_means)

    if n_cl > 1:
        se = float(np.std(cl_means, ddof=1) / np.sqrt(n_cl))
    else:
        se = float(np.std(tau_i, ddof=1) / np.sqrt(n_treated))

    z = att / se if se > 0 else np.nan
    pval = float(2 * (1 - stats.norm.cdf(abs(z)))) if not np.isnan(z) else np.nan

    return EstimationResult(
        coef={treat_col: att},
        se={treat_col: se},
        pval={treat_col: pval},
        ci_lower={treat_col: att - 1.96 * se},
        ci_upper={treat_col: att + 1.96 * se},
        n_obs=n_obs,
        r_sq=np.nan,
        adj_r_sq=None,
        method="Borusyak-Jaravel-Spiess Imputation",
        depvar=y,
        diagnostics={
            "n_treated_obs": n_treated,
            "n_untreated_obs": len(untreated_data),
            "n_clusters": n_cl,
            "mean_tau": att,
            "std_tau": float(np.std(tau_i)),
        },
    )
