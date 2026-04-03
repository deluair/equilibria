"""Special Economic Zone (SEZ) economics analysis.

Difference-in-differences for SEZ establishment effects, FDI attraction
measurement, spillover estimation to surrounding areas, and agglomeration
shadow detection.

DiD for SEZ effects (Alder, Lagakos & Ohanian 2019):
    Y_it = alpha + beta * SEZ_i * Post_t + gamma * X_it + mu_i + lambda_t + e_it

where Y is output/employment/wages, SEZ_i indicates treated zones,
Post_t indicates post-establishment periods, and mu_i/lambda_t are
unit/time fixed effects. beta is the ATT of SEZ establishment.

FDI attraction:
    FDI_i = f(tax_incentives, infrastructure, labor_cost, governance, proximity)
    Measured as FDI inflows per zone vs. counterfactual (non-SEZ areas).

Spillovers (Kline & Moretti 2014):
    Examine whether SEZ benefits diffuse to surrounding regions via
    employment spillovers, wage spillovers, and firm entry in adjacent areas.
    Spillover gradient: effect decays with distance from SEZ boundary.

Agglomeration shadow (Fujita & Ogawa 1982):
    Large SEZs may "crowd out" activity in nearby areas by attracting firms
    and workers. The shadow zone is the area within a certain radius where
    economic activity is depressed relative to farther locations.

References:
    Alder, S., Lagakos, D. & Ohanian, L. (2019). Labor Market Regulations
        and the Cost of Corruption: Evidence from the Chinese Manufacturing
        Sector. WP.
    Kline, P. & Moretti, E. (2014). Local Economic Development, Agglomeration
        Economies, and the Big Push. QJE 129(1): 275-331.
    Wang, J. (2013). The Economic Impact of Special Economic Zones: Evidence
        from Chinese Municipalities. Journal of Development Economics 101.

Score: weak SEZ effect + negative spillovers + strong agglomeration shadow -> STRESS.
"""

import json

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class SEZEconomics(LayerBase):
    layer_id = "l11"
    name = "SEZ Economics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        # --- DiD for SEZ establishment effects ---
        did_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'sez_panel'
            ORDER BY dp.date
            """,
            (country,),
        )

        did_result = None
        if did_rows and len(did_rows) >= 20:
            # Parse panel: unit, period, treated, post, outcome, controls
            units = {}
            for row in did_rows:
                outcome = row["value"]
                if outcome is None:
                    continue
                meta = json.loads(row["metadata"]) if row.get("metadata") else {}
                unit = meta.get("unit")
                period = meta.get("period")
                treated = meta.get("treated", 0)
                post = meta.get("post", 0)
                if unit is None or period is None:
                    continue
                units.setdefault(unit, []).append({
                    "period": int(period),
                    "treated": int(treated),
                    "post": int(post),
                    "outcome": float(outcome),
                })

            # Flatten to arrays
            outcomes = []
            treated_arr = []
            post_arr = []
            interaction = []
            unit_ids = []
            period_ids = []
            unique_units = sorted(units.keys())
            unit_map = {u: i for i, u in enumerate(unique_units)}
            all_periods = set()

            for unit, obs_list in units.items():
                for obs in obs_list:
                    outcomes.append(obs["outcome"])
                    treated_arr.append(obs["treated"])
                    post_arr.append(obs["post"])
                    interaction.append(obs["treated"] * obs["post"])
                    unit_ids.append(unit_map[unit])
                    period_ids.append(obs["period"])
                    all_periods.add(obs["period"])

            n_obs = len(outcomes)
            if n_obs >= 20:
                y = np.array(outcomes)
                treat = np.array(treated_arr, dtype=float)
                post = np.array(post_arr, dtype=float)
                did_term = np.array(interaction, dtype=float)

                # TWFE: Y = alpha + beta*D_it + unit_FE + time_FE + e
                n_units = len(unique_units)
                n_periods = len(all_periods)
                period_list = sorted(all_periods)
                period_map = {p: i for i, p in enumerate(period_list)}

                # Build dummies (drop first unit and first period for identification)
                X_parts = [np.ones((n_obs, 1)), did_term.reshape(-1, 1)]

                if n_units > 1:
                    unit_dummies = np.zeros((n_obs, n_units - 1))
                    for i, uid in enumerate(unit_ids):
                        if uid > 0:
                            unit_dummies[i, uid - 1] = 1.0
                    X_parts.append(unit_dummies)

                if n_periods > 1:
                    period_dummies = np.zeros((n_obs, n_periods - 1))
                    for i, pid in enumerate(period_ids):
                        pidx = period_map[pid]
                        if pidx > 0:
                            period_dummies[i, pidx - 1] = 1.0
                    X_parts.append(period_dummies)

                X_did = np.column_stack(X_parts)
                beta_did = np.linalg.lstsq(X_did, y, rcond=None)[0]
                att = float(beta_did[1])  # DiD coefficient

                # Standard error (HC1)
                resid_did = y - X_did @ beta_did
                k_did = X_did.shape[1]
                XtX_inv = np.linalg.pinv(X_did.T @ X_did)
                omega = np.diag(resid_did ** 2) * (n_obs / max(n_obs - k_did, 1))
                V_did = XtX_inv @ (X_did.T @ omega @ X_did) @ XtX_inv
                se_att = float(np.sqrt(max(V_did[1, 1], 0.0)))

                t_stat = att / se_att if se_att > 0 else 0.0
                p_val = 2.0 * (1.0 - stats.t.cdf(abs(t_stat), df=max(n_obs - k_did, 1)))

                # Pre-trend test: restrict to pre-period, test for differential trend
                pre_mask = np.array(post_arr) == 0
                pre_trend_pval = None
                if pre_mask.sum() >= 10:
                    y_pre = y[pre_mask]
                    X_pre = np.column_stack([
                        np.ones(pre_mask.sum()),
                        treat[pre_mask],
                        np.array(period_ids)[pre_mask].astype(float),
                        treat[pre_mask] * np.array(period_ids)[pre_mask].astype(float),
                    ])
                    beta_pre = np.linalg.lstsq(X_pre, y_pre, rcond=None)[0]
                    resid_pre = y_pre - X_pre @ beta_pre
                    ss_res_pre = np.sum(resid_pre ** 2)
                    ss_tot_pre = np.sum((y_pre - y_pre.mean()) ** 2)
                    if ss_res_pre > 0 and len(y_pre) > 4:
                        f_pre = ((ss_tot_pre - ss_res_pre) / 1) / (ss_res_pre / (len(y_pre) - 4))
                        pre_trend_pval = 1.0 - stats.f.cdf(abs(f_pre), 1, len(y_pre) - 4)

                did_result = {
                    "att": round(att, 4),
                    "se": round(se_att, 4),
                    "t_stat": round(t_stat, 4),
                    "p_value": round(float(p_val), 6),
                    "significant": p_val < 0.05,
                    "n_obs": n_obs,
                    "n_units": n_units,
                    "n_periods": n_periods,
                    "pre_trend_pval": round(float(pre_trend_pval), 4) if pre_trend_pval is not None else None,
                    "parallel_trends_ok": pre_trend_pval is not None and pre_trend_pval > 0.10,
                }

        # --- FDI attraction ---
        fdi_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'sez_fdi'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        fdi_result = None
        if fdi_rows and len(fdi_rows) >= 5:
            fdi_sez = []
            fdi_non_sez = []
            for row in fdi_rows:
                meta = json.loads(row["metadata"]) if row.get("metadata") else {}
                fdi = row["value"]
                if fdi is None:
                    continue
                is_sez = meta.get("is_sez", 0)
                if is_sez:
                    fdi_sez.append(float(fdi))
                else:
                    fdi_non_sez.append(float(fdi))

            if fdi_sez and fdi_non_sez:
                mean_sez = float(np.mean(fdi_sez))
                mean_non = float(np.mean(fdi_non_sez))
                ratio = mean_sez / mean_non if mean_non > 0 else None
                t_fdi, p_fdi = stats.ttest_ind(fdi_sez, fdi_non_sez, equal_var=False)

                fdi_result = {
                    "mean_fdi_sez": round(mean_sez, 2),
                    "mean_fdi_non_sez": round(mean_non, 2),
                    "fdi_ratio": round(ratio, 3) if ratio else None,
                    "t_stat": round(float(t_fdi), 4),
                    "p_value": round(float(p_fdi), 6),
                    "n_sez": len(fdi_sez),
                    "n_non_sez": len(fdi_non_sez),
                }

        # --- Spillover estimation ---
        spillover_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'sez_spillover'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        spillover_result = None
        if spillover_rows and len(spillover_rows) >= 10:
            outcomes = []
            distances = []
            for row in spillover_rows:
                meta = json.loads(row["metadata"]) if row.get("metadata") else {}
                outcome = row["value"]
                dist = meta.get("distance_to_sez")
                if outcome is not None and dist is not None and dist >= 0:
                    outcomes.append(float(outcome))
                    distances.append(float(dist))

            if len(outcomes) >= 10:
                outcomes_arr = np.array(outcomes)
                distances_arr = np.array(distances)

                # Distance gradient: outcome = a + b*ln(1+dist) + e
                ln_dist = np.log(1.0 + distances_arr)
                X_spill = np.column_stack([np.ones(len(outcomes)), ln_dist])
                beta_spill = np.linalg.lstsq(X_spill, outcomes_arr, rcond=None)[0]
                gradient = float(beta_spill[1])

                resid_spill = outcomes_arr - X_spill @ beta_spill
                ss_res = np.sum(resid_spill ** 2)
                ss_tot = np.sum((outcomes_arr - outcomes_arr.mean()) ** 2)
                r2_spill = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

                spillover_result = {
                    "distance_gradient": round(gradient, 4),
                    "positive_spillover": gradient > 0,
                    "r_squared": round(r2_spill, 4),
                    "n_obs": len(outcomes),
                    "max_distance": round(float(distances_arr.max()), 2),
                }

        # --- Agglomeration shadow ---
        shadow_result = None
        if spillover_rows and len(spillover_rows) >= 15:
            # Re-use spillover data: check for non-monotonic decay
            # Shadow = dip in activity at intermediate distances
            if len(outcomes) >= 15:
                # Sort by distance
                sort_idx = np.argsort(distances_arr)
                sorted_outcomes = outcomes_arr[sort_idx]

                # Split into 3 distance bands
                n_band = len(sorted_outcomes) // 3
                if n_band >= 3:
                    near = sorted_outcomes[:n_band]
                    mid = sorted_outcomes[n_band:2 * n_band]
                    far = sorted_outcomes[2 * n_band:]

                    mean_near = float(np.mean(near))
                    mean_mid = float(np.mean(mid))
                    mean_far = float(np.mean(far))

                    # Shadow exists if mid-band is lower than both near and far
                    shadow_exists = mean_mid < mean_near and mean_mid < mean_far

                    shadow_result = {
                        "near_band_mean": round(mean_near, 4),
                        "mid_band_mean": round(mean_mid, 4),
                        "far_band_mean": round(mean_far, 4),
                        "shadow_detected": shadow_exists,
                        "shadow_depth": round(
                            (min(mean_near, mean_far) - mean_mid) / max(abs(mean_mid), 1e-10), 4
                        ) if shadow_exists else 0.0,
                    }

        # --- Score ---
        scores = []

        # DiD: insignificant or negative ATT is concerning
        if did_result:
            if did_result["significant"] and did_result["att"] > 0:
                scores.append(15.0)  # SEZ working well
            elif did_result["att"] > 0:
                scores.append(40.0)  # Positive but insignificant
            elif did_result["att"] <= 0:
                scores.append(75.0)  # SEZ not working

        # FDI
        if fdi_result:
            ratio = fdi_result.get("fdi_ratio")
            if ratio and ratio > 3.0:
                scores.append(15.0)
            elif ratio and ratio > 1.5:
                scores.append(35.0)
            else:
                scores.append(65.0)

        # Spillovers
        if spillover_result:
            if spillover_result["positive_spillover"]:
                scores.append(20.0)
            else:
                scores.append(70.0)  # Negative spillover

        # Shadow
        if shadow_result:
            if shadow_result["shadow_detected"]:
                scores.append(65.0 + shadow_result["shadow_depth"] * 20.0)
            else:
                scores.append(25.0)

        score = float(np.mean(scores)) if scores else 50.0
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "did_effect": did_result,
            "fdi_attraction": fdi_result,
            "spillover": spillover_result,
            "agglomeration_shadow": shadow_result,
        }
