"""Gentrification: displacement risk, rent gap theory (Smith 1979),
neighborhood change detection, and anti-displacement policy effectiveness.

Key frameworks:

1. Rent gap theory (Smith 1979, 1987):
   Rent gap = capitalized_ground_rent_potential - actual_ground_rent
   Gentrification becomes profitable when rent gap exceeds reinvestment cost.
   Gentrification pressure = rent_gap / median_rent (normalized)
   High rent gap + rising investment -> gentrification onset signal.

2. Displacement risk index (Urban Displacement Project, 2019):
   Composite of: rent burden, rent change, income change, demographic shifts,
   new construction, tenure composition change.
   Low-income renters with high rent burden + large rent increase -> high risk.

3. Neighborhood change detection:
   Transition matrix: tract-level socioeconomic status transitions over 10yr.
   Typical categories: distressed, transitional, stable, gentrifying, advanced.
   Classification: Freeman (2005) criteria - low initial income + college growth
   + real income growth exceeding metro median.

4. Anti-displacement policy effectiveness:
   Community Land Trusts (CLT): permanently affordable units as % of housing stock.
   Rent stabilization: coverage rate (% renter units covered) and rent change constraint.
   Inclusionary zoning: affordable unit requirement % in new developments.
   Tenant protections: just-cause eviction, right-to-return, relocation assistance.

References:
    Smith, N. (1979). Toward a Theory of Gentrification. JAIP 45(4).
    Smith, N. (1987). Gentrification and the Rent Gap. AAAG 77(3).
    Freeman, L. (2005). Displacement or Succession? Urban Affairs Review 40(4).
    Urban Displacement Project (2019). Methodology Report. UC Berkeley.
    Marcuse, P. (1985). Gentrification, Abandonment, and Displacement. JUWH 9(2).

Sources: US Census ACS, HMDA, HUD, local property assessor databases.
"""

from __future__ import annotations

import json

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class Gentrification(LayerBase):
    layer_id = "l11"
    name = "Gentrification"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        results: dict = {"country": country}

        # --- Rent gap ---
        rent_gap_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('RENT_GAP_INDEX', 'CAPITALIZED_GROUND_RENT_POTENTIAL')
            ORDER BY dp.date
            """,
            (country,),
        )

        rent_gap_result = None
        if rent_gap_rows:
            latest = rent_gap_rows[-1]
            rent_gap_idx = float(latest["value"])
            meta = json.loads(latest["metadata"]) if latest.get("metadata") else {}

            potential_rent = meta.get("potential_capitalized_rent", None)
            actual_rent = meta.get("actual_capitalized_rent", None)
            investment_threshold = meta.get("reinvestment_cost_threshold", None)

            if potential_rent and actual_rent and float(actual_rent) > 0:
                rent_gap_value = float(potential_rent) - float(actual_rent)
                rent_gap_normalized = rent_gap_value / float(actual_rent)
                profitable = (
                    rent_gap_value > float(investment_threshold)
                    if investment_threshold is not None
                    else None
                )
            else:
                rent_gap_value = None
                rent_gap_normalized = None
                profitable = None

            if len(rent_gap_rows) >= 3:
                rg_arr = np.array([float(r["value"]) for r in rent_gap_rows[-10:]])
                t_arr = np.arange(len(rg_arr))
                slope_rg, _, r_rg, p_rg, _ = sp_stats.linregress(t_arr, rg_arr)
            else:
                slope_rg, r_rg, p_rg = None, None, None

            rent_gap_result = {
                "rent_gap_index": round(rent_gap_idx, 4),
                "rent_gap_absolute": round(rent_gap_value, 2) if rent_gap_value is not None else None,
                "rent_gap_normalized": round(rent_gap_normalized, 4) if rent_gap_normalized is not None else None,
                "gentrification_profitable": profitable,
                "trend_slope": round(float(slope_rg), 5) if slope_rg is not None else None,
                "widening": slope_rg > 0 if slope_rg is not None else None,
            }
        results["rent_gap"] = rent_gap_result or {"error": "no rent gap data"}

        # --- Displacement risk index ---
        displacement_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('DISPLACEMENT_RISK_INDEX', 'RENTER_BURDEN_PCT')
            ORDER BY dp.date
            """,
            (country,),
        )

        displacement_result = None
        if displacement_rows:
            latest_d = displacement_rows[-1]
            dri = float(latest_d["value"])
            meta = json.loads(latest_d["metadata"]) if latest_d.get("metadata") else {}

            rent_burden_pct = meta.get("rent_burden_pct", None)
            rent_change_3yr = meta.get("rent_change_3yr", None)
            low_income_renter_share = meta.get("low_income_renter_share", None)
            demographic_shift_score = meta.get("demographic_shift_score", None)

            if len(displacement_rows) >= 3:
                dri_arr = np.array([float(r["value"]) for r in displacement_rows[-10:]])
                t_arr = np.arange(len(dri_arr))
                slope_dri, _, _, _, _ = sp_stats.linregress(t_arr, dri_arr)
            else:
                slope_dri = None

            displacement_result = {
                "displacement_risk_index": round(dri, 4),
                "rent_burden_pct": round(float(rent_burden_pct), 2) if rent_burden_pct is not None else None,
                "rent_change_3yr_pct": round(float(rent_change_3yr), 2) if rent_change_3yr is not None else None,
                "low_income_renter_share": round(float(low_income_renter_share), 4) if low_income_renter_share is not None else None,
                "demographic_shift_score": round(float(demographic_shift_score), 4) if demographic_shift_score is not None else None,
                "trend_slope": round(float(slope_dri), 5) if slope_dri is not None else None,
                "worsening": slope_dri > 0 if slope_dri is not None else None,
                "risk_level": (
                    "critical" if dri > 0.75
                    else "high" if dri > 0.55
                    else "moderate" if dri > 0.35
                    else "low"
                ),
            }
        results["displacement_risk"] = displacement_result or {"error": "no displacement risk data"}

        # --- Neighborhood change detection (Freeman criteria) ---
        nbhd_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id LIKE '%NEIGHBORHOOD_CHANGE%'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        nbhd_result = None
        if nbhd_rows and len(nbhd_rows) >= 5:
            statuses: dict[str, int] = {}
            for r in nbhd_rows:
                meta = json.loads(r["metadata"]) if r.get("metadata") else {}
                status = meta.get("neighborhood_status", "unknown")
                statuses[status] = statuses.get(status, 0) + 1

            total_tracts = sum(statuses.values())
            gentrifying_share = statuses.get("gentrifying", 0) / total_tracts if total_tracts > 0 else 0.0
            displaced_share = statuses.get("displaced", 0) / total_tracts if total_tracts > 0 else 0.0
            distressed_share = statuses.get("distressed", 0) / total_tracts if total_tracts > 0 else 0.0

            nbhd_result = {
                "total_tracts": total_tracts,
                "status_distribution": statuses,
                "gentrifying_share": round(gentrifying_share, 4),
                "displaced_share": round(displaced_share, 4),
                "distressed_share": round(distressed_share, 4),
                "at_risk_share": round(gentrifying_share + displaced_share, 4),
            }
        results["neighborhood_change"] = nbhd_result or {"error": "no neighborhood change data"}

        # --- Anti-displacement policy effectiveness ---
        policy_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('ANTI_DISPLACEMENT_POLICY_INDEX', 'RENT_STABILIZATION_COVERAGE')
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        policy_result = None
        if policy_rows:
            meta = json.loads(policy_rows[0]["metadata"]) if policy_rows[0].get("metadata") else {}
            policy_idx = float(policy_rows[0]["value"])
            rent_stab_coverage = meta.get("rent_stabilization_coverage", None)
            clt_share = meta.get("clt_affordable_share", None)
            inclusionary_requirement = meta.get("inclusionary_zoning_pct", None)
            just_cause = meta.get("just_cause_eviction", None)

            policy_result = {
                "policy_index": round(policy_idx, 4),
                "rent_stabilization_coverage": round(float(rent_stab_coverage), 4) if rent_stab_coverage is not None else None,
                "clt_share_of_stock": round(float(clt_share), 4) if clt_share is not None else None,
                "inclusionary_zoning_pct": round(float(inclusionary_requirement), 2) if inclusionary_requirement is not None else None,
                "just_cause_eviction": bool(just_cause) if just_cause is not None else None,
                "effectiveness": (
                    "strong" if policy_idx > 0.70
                    else "moderate" if policy_idx > 0.40
                    else "weak"
                ),
            }
        results["anti_displacement_policy"] = policy_result or {"error": "no policy data"}

        # --- Score ---
        score = 25.0

        # High displacement risk
        if displacement_result and not displacement_result.get("error"):
            dri_v = displacement_result["displacement_risk_index"]
            score += dri_v * 30.0
            if displacement_result.get("worsening"):
                score += 5.0

        # Rent gap widening
        if rent_gap_result and not rent_gap_result.get("error"):
            rgi = rent_gap_result["rent_gap_index"]
            if rent_gap_result.get("gentrification_profitable"):
                score += 10.0
            score += min(10.0, rgi * 5.0)

        # Large at-risk neighborhood share
        if nbhd_result and not nbhd_result.get("error"):
            at_risk = nbhd_result["at_risk_share"]
            score += min(15.0, at_risk * 30.0)

        # Weak anti-displacement policy
        if policy_result and not policy_result.get("error"):
            if policy_result["effectiveness"] == "weak":
                score += 10.0
            elif policy_result["effectiveness"] == "strong":
                score -= 5.0

        score = float(np.clip(score, 0.0, 100.0))
        return {"score": round(score, 1), "results": results}
