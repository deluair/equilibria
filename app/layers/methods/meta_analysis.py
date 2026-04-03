"""Meta-analysis: fixed/random effects, DerSimonian-Laird, Egger test, trim-and-fill.

Methodology
-----------
**Fixed Effects (FE) Meta-Analysis**:
Assumes all studies share a common true effect size theta.
    Weighted mean: theta_FE = sum_i w_i * y_i / sum_i w_i
    where w_i = 1 / v_i (inverse variance weights), v_i = within-study variance.
    Variance: Var(theta_FE) = 1 / sum_i w_i
    95% CI: theta_FE +/- 1.96 * sqrt(Var)

FE is efficient if heterogeneity is absent, but underestimates uncertainty otherwise.

**Random Effects (RE) Meta-Analysis (DerSimonian-Laird 1986)**:
Assumes theta_i ~ N(theta, tau^2), between-study heterogeneity tau^2.
    Q statistic: Q = sum_i w_i * (y_i - theta_FE)^2 ~ chi^2(k-1) under H0: tau^2=0
    tau^2_DL = max(0, (Q - (k-1)) / C)
    C = sum_i w_i - sum_i w_i^2 / sum_i w_i

    RE weights: w_i* = 1 / (v_i + tau^2_DL)
    theta_RE = sum_i w_i* * y_i / sum_i w_i*
    Var(theta_RE) = 1 / sum_i w_i*

    I^2 = max(0, (Q - (k-1)) / Q) * 100%  (% heterogeneity due to between-study variation)
    Interpretation: I^2 > 75% = high heterogeneity

**Heterogeneity Tests**:
    Cochran Q: chi^2(k-1) under H0: tau^2=0
    I-squared: % total variation from heterogeneity
    H-squared: Q / (k-1), > 1 indicates heterogeneity

**Funnel Plot Asymmetry (Egger Test)** (Egger et al. 1997):
    Regress standardized effect on precision:
        y_i / SE_i = a + b * (1 / SE_i) + error
    H0: a=0 (no asymmetry). t-test on intercept a with k-2 df.
    Significant a -> funnel asymmetry -> possible publication bias.

**Trim-and-Fill (Duval & Tweedie 2000)**:
    Iteratively remove ("trim") most extreme small studies from right,
    recompute center, until symmetric. Add ("fill") imputed mirror-image
    studies on left. Adjusted effect accounts for missing studies.

    Algorithm L0 (linear): estimate number of missing studies R0:
        R0 = round((4S - k * (k-1) * (k + 1) / 4) / (k - 1) - 1/2)
    where S = sum of ranks of studies to the right of corrected mean.

Score: large tau^2 + significant Egger + large trim-and-fill adjustment -> STRESS.
Low heterogeneity + no publication bias -> STABLE.

References:
    DerSimonian, R. & Laird, N. (1986). Meta-Analysis in Clinical Trials.
        Controlled Clinical Trials 7(3): 177-188.
    Egger, M., Smith, G.D., Schneider, M. & Minder, C. (1997). Bias in
        Meta-Analysis Detected by a Simple, Graphical Test.
        British Medical Journal 315: 629-634.
    Duval, S. & Tweedie, R. (2000). Trim and Fill: A Simple Funnel-Plot-Based
        Method of Testing and Adjusting for Publication Bias in Meta-Analysis.
        Biometrics 56(2): 455-463.
"""

import json
import math

import numpy as np
from scipy.stats import chi2, norm, t as t_dist

from app.layers.base import LayerBase


def _fixed_effects(y: np.ndarray, v: np.ndarray) -> dict:
    """Inverse-variance weighted FE estimator."""
    w = 1.0 / np.maximum(v, 1e-12)
    theta = float(np.sum(w * y) / np.sum(w))
    var = float(1.0 / np.sum(w))
    se = math.sqrt(var)
    z = theta / se if se > 0 else 0
    return {
        "theta": round(theta, 4),
        "se": round(se, 4),
        "ci_95": [round(theta - 1.96 * se, 4), round(theta + 1.96 * se, 4)],
        "z": round(float(z), 4),
        "p_value": round(2 * float(norm.sf(abs(z))), 4),
    }


def _dersimonian_laird(y: np.ndarray, v: np.ndarray) -> dict:
    """DerSimonian-Laird random effects meta-analysis."""
    k = len(y)
    w = 1.0 / np.maximum(v, 1e-12)
    theta_fe = float(np.sum(w * y) / np.sum(w))

    # Q statistic
    Q = float(np.sum(w * (y - theta_fe) ** 2))
    df = k - 1

    # tau^2 DL
    C = float(np.sum(w) - np.sum(w ** 2) / np.sum(w))
    tau2 = max(0.0, (Q - df) / C) if C > 0 else 0.0

    # RE weights
    w_star = 1.0 / np.maximum(v + tau2, 1e-12)
    theta_re = float(np.sum(w_star * y) / np.sum(w_star))
    var_re = float(1.0 / np.sum(w_star))
    se_re = math.sqrt(var_re)
    z_re = theta_re / se_re if se_re > 0 else 0

    # Heterogeneity statistics
    I2 = max(0.0, (Q - df) / Q * 100) if Q > 0 else 0.0
    H2 = Q / df if df > 0 else 1.0

    # Q test p-value
    q_pval = float(chi2.sf(Q, df=df)) if df > 0 else 1.0

    return {
        "theta": round(theta_re, 4),
        "se": round(se_re, 4),
        "ci_95": [round(theta_re - 1.96 * se_re, 4), round(theta_re + 1.96 * se_re, 4)],
        "z": round(float(z_re), 4),
        "p_value": round(2 * float(norm.sf(abs(z_re))), 4),
        "tau_squared": round(float(tau2), 6),
        "tau": round(float(math.sqrt(tau2)), 4),
        "Q_statistic": round(float(Q), 4),
        "Q_df": int(df),
        "Q_pvalue": round(q_pval, 4),
        "I_squared": round(float(I2), 1),
        "H_squared": round(float(H2), 3),
        "heterogeneity_level": (
            "high" if I2 > 75 else "moderate" if I2 > 50 else "low" if I2 > 25 else "negligible"
        ),
    }


def _egger_test(y: np.ndarray, v: np.ndarray) -> dict:
    """Egger test for funnel plot asymmetry."""
    k = len(y)
    if k < 4:
        return {"error": "need >= 4 studies"}
    se = np.sqrt(np.maximum(v, 1e-12))
    precision = 1.0 / se
    std_effect = y / se
    # Regress std_effect on precision
    X = np.column_stack([np.ones(k), precision])
    try:
        beta, _, _, _ = np.linalg.lstsq(X, std_effect, rcond=None)
    except np.linalg.LinAlgError:
        return {"error": "regression failed"}
    resid = std_effect - X @ beta
    s2 = float(np.sum(resid ** 2)) / max(k - 2, 1)
    XtX_inv = np.linalg.pinv(X.T @ X)
    se_intercept = math.sqrt(s2 * XtX_inv[0, 0]) if s2 > 0 else 1e-10
    t_stat = beta[0] / se_intercept if se_intercept > 0 else 0.0
    p_val = 2 * float(t_dist.sf(abs(t_stat), df=k - 2))
    return {
        "intercept": round(float(beta[0]), 4),
        "intercept_se": round(float(se_intercept), 4),
        "t_statistic": round(float(t_stat), 4),
        "p_value": round(p_val, 4),
        "asymmetric": p_val < 0.10,
    }


def _trim_and_fill(y: np.ndarray, v: np.ndarray) -> dict:
    """Duval-Tweedie trim-and-fill algorithm L0."""
    k = len(y)
    if k < 4:
        return {"imputed_studies": 0, "adjusted_theta": None}

    # Iterate: trim, re-center, estimate R0
    for _ in range(20):  # max 20 iterations
        re = _dersimonian_laird(y, v)
        center = re["theta"]
        # Sort by deviation from center
        deviations = y - center
        ranks = np.argsort(np.argsort(deviations)) + 1  # rank 1 = smallest
        n_right = int(np.sum(deviations > 0))
        if n_right == 0:
            break
        # S = sum of ranks of right-side studies
        right_mask = deviations > 0
        S = int(np.sum(ranks[right_mask]))
        R0_raw = (4 * S - k * (k - 1) * (k + 1) / 4) / (k - 1) - 0.5
        R0 = max(0, round(float(R0_raw)))
        if R0 == 0:
            break
        # Trim R0 most extreme right studies
        extreme_right_idx = np.argsort(deviations)[-R0:]
        keep = np.ones(k, dtype=bool)
        keep[extreme_right_idx] = False
        if np.sum(keep) < 3:
            break
        y_trimmed = y[keep]
        v_trimmed = v[keep]
        re_trim = _dersimonian_laird(y_trimmed, v_trimmed)
        center = re_trim["theta"]
        break
    else:
        R0 = 0
        center = _dersimonian_laird(y, v)["theta"]

    if R0 == 0:
        return {
            "imputed_studies": 0,
            "adjusted_theta": round(float(center), 4),
            "original_theta": round(float(center), 4),
            "theta_change": 0.0,
        }

    # Fill: add R0 mirror-image studies
    # Most extreme right studies (trimmed) mirrored to left
    deviations_all = y - center
    right_idx = np.argsort(deviations_all)[-R0:]
    y_mirrors = 2 * center - y[right_idx]
    v_mirrors = v[right_idx]

    y_filled = np.concatenate([y, y_mirrors])
    v_filled = np.concatenate([v, v_mirrors])

    re_filled = _dersimonian_laird(y_filled, v_filled)
    original_re = _dersimonian_laird(y, v)

    return {
        "imputed_studies": int(R0),
        "adjusted_theta": round(float(re_filled["theta"]), 4),
        "adjusted_ci_95": re_filled["ci_95"],
        "original_theta": round(float(original_re["theta"]), 4),
        "theta_change": round(abs(re_filled["theta"] - original_re["theta"]), 4),
        "k_filled": len(y_filled),
    }


class MetaAnalysis(LayerBase):
    layer_id = "l18"
    name = "Meta-Analysis"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'meta_analysis'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 4:
            return {"score": None, "signal": "UNAVAILABLE", "error": "need >= 4 studies"}

        y_list, v_list, study_ids, study_names = [], [], [], []
        for row in rows:
            if row["value"] is None:
                continue
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            # Effect size (e.g., standardized mean difference, log OR)
            y_val = float(row["value"])
            # Within-study variance (from SE or CI)
            se = meta.get("se")
            ci_lo = meta.get("ci_lo")
            ci_hi = meta.get("ci_hi")
            n_study = meta.get("n")

            if se is not None:
                v_val = float(se) ** 2
            elif ci_lo is not None and ci_hi is not None:
                v_val = ((float(ci_hi) - float(ci_lo)) / (2 * 1.96)) ** 2
            elif n_study is not None:
                v_val = 1.0 / max(float(n_study), 1)
            else:
                continue  # Cannot estimate variance

            y_list.append(y_val)
            v_list.append(max(v_val, 1e-12))
            study_ids.append(row["date"])
            study_names.append(meta.get("study", f"Study {len(y_list)}"))

        k = len(y_list)
        if k < 4:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient studies with variance info"}

        y = np.array(y_list)
        v = np.array(v_list)

        # --- Fixed effects ---
        fe_results = _fixed_effects(y, v)

        # --- Random effects (DL) ---
        re_results = _dersimonian_laird(y, v)

        # --- Egger test ---
        egger_results = _egger_test(y, v)

        # --- Trim and fill ---
        tnf_results = _trim_and_fill(y, v)

        # --- Study-level summary ---
        studies = []
        w_star = 1.0 / np.maximum(v + re_results["tau_squared"], 1e-12)
        w_pct = w_star / np.sum(w_star) * 100
        for i in range(k):
            se_i = math.sqrt(float(v[i]))
            studies.append({
                "study": study_names[i],
                "effect_size": round(float(y[i]), 4),
                "se": round(se_i, 4),
                "ci_95": [round(float(y[i]) - 1.96 * se_i, 4),
                          round(float(y[i]) + 1.96 * se_i, 4)],
                "weight_pct_re": round(float(w_pct[i]), 1),
            })

        # --- Score ---
        score = 15.0

        # Heterogeneity level
        i2 = re_results.get("I_squared", 0) or 0
        if i2 > 75:
            score += 35
        elif i2 > 50:
            score += 20
        elif i2 > 25:
            score += 10

        # Publication bias
        if egger_results.get("asymmetric"):
            score += 20

        # Trim-and-fill adjustment size
        theta_change = tnf_results.get("theta_change", 0) or 0
        if theta_change > 0.1 * abs(re_results["theta"]) if re_results["theta"] != 0 else False:
            score += 15

        # tau magnitude relative to effect size
        tau = re_results.get("tau", 0) or 0
        theta_abs = abs(re_results.get("theta", 1)) or 1
        if tau / theta_abs > 0.5:
            score += 10

        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "k_studies": k,
            "fixed_effects": fe_results,
            "random_effects_dl": re_results,
            "egger_test": egger_results,
            "trim_and_fill": tnf_results,
            "studies": studies,
        }
