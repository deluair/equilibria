"""Rural-urban migration: Harris-Todaro expected wage differential,
push-pull factor analysis, urbanization speed, informal sector absorption.

Key frameworks:

1. Harris-Todaro model (Harris & Todaro 1970):
   Migration equilibrium condition:
   w_r = p * w_u
   where w_r = rural wage, p = probability of finding urban formal job,
         w_u = urban formal wage.
   p = L_u / N_u  (employed urban formal / total urban labor force)
   Expected urban wage = p * w_u
   Net migration impulse = E[w_u] - w_r > 0 -> rural-to-urban flow.
   Todaro paradox: migration continues even with urban unemployment.

2. Push-pull factor decomposition:
   Push factors (origin): low agricultural productivity, land scarcity,
   natural disaster exposure, conflict, poverty.
   Pull factors (destination): wage premium, amenities, social networks,
   services access, industrial agglomeration.
   Gravity-augmented: flow_ij = k * (push_i * pull_j) / dist_ij^alpha

3. Urbanization speed:
   Urban population growth rate relative to total population growth.
   Urbanization speed = d(urban_share)/dt
   Rapid urbanization (>2%/yr) -> infrastructure stress, informality surge.
   Historical benchmark: East Asian rapid urbanization 1960-1990.

4. Informal sector absorption:
   Informal employment share = informal_workers / total_urban_employment
   Lewis (1954) dual-sector: surplus rural labor absorbed until wage equalized.
   Absorption capacity = formal_job_creation_rate / rural-urban_migrant_inflow
   If capacity < 1: informality grows; distress migration signal.

References:
    Harris, J. & Todaro, M. (1970). Migration, Unemployment and Development.
        American Economic Review, 60(1), 126-142.
    Lewis, W.A. (1954). Economic Development with Unlimited Supplies of Labour.
        Manchester School, 22(2), 139-191.
    Todaro, M. (1969). A Model of Labor Migration and Urban Unemployment.
        American Economic Review, 59(1), 138-148.
    Lall, S., Selod, H. & Shalizi, Z. (2006). Rural-Urban Migration in LDCs.
        World Bank Policy Research WP 3915.

Sources: ILO, World Bank WDI, UN DESA World Urbanization Prospects.
"""

from __future__ import annotations

import json

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class RuralUrbanMigration(LayerBase):
    layer_id = "l11"
    name = "Rural Urban Migration"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        results: dict = {"country": country}

        # --- Harris-Todaro expected wage differential ---
        wage_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('RURAL_URBAN_WAGE_RATIO', 'HARRIS_TODARO_PARAMS')
            ORDER BY dp.date
            """,
            (country,),
        )

        ht_result = None
        if wage_rows:
            latest = wage_rows[-1]
            wage_ratio = float(latest["value"])  # rural/urban formal wage
            meta = json.loads(latest["metadata"]) if latest.get("metadata") else {}

            w_rural = meta.get("rural_wage", None)
            w_urban_formal = meta.get("urban_formal_wage", None)
            urban_employment_rate = meta.get("urban_formal_employment_rate", None)

            expected_urban_wage = None
            ht_differential = None
            if w_urban_formal and urban_employment_rate:
                expected_urban_wage = float(w_urban_formal) * float(urban_employment_rate)
                if w_rural:
                    ht_differential = expected_urban_wage - float(w_rural)

            # Trend in wage ratio
            if len(wage_rows) >= 3:
                wr_arr = np.array([float(r["value"]) for r in wage_rows[-10:]])
                t_arr = np.arange(len(wr_arr))
                slope_wr, _, _, p_wr, _ = sp_stats.linregress(t_arr, wr_arr)
            else:
                slope_wr, p_wr = None, None

            ht_result = {
                "rural_urban_wage_ratio": round(wage_ratio, 4),
                "rural_wage": round(float(w_rural), 2) if w_rural is not None else None,
                "urban_formal_wage": round(float(w_urban_formal), 2) if w_urban_formal is not None else None,
                "urban_formal_employment_rate": round(float(urban_employment_rate), 4) if urban_employment_rate is not None else None,
                "expected_urban_wage": round(expected_urban_wage, 2) if expected_urban_wage is not None else None,
                "ht_differential": round(ht_differential, 2) if ht_differential is not None else None,
                "migration_incentive": ht_differential > 0 if ht_differential is not None else None,
                "todaro_paradox_active": (
                    ht_differential > 0 and float(urban_employment_rate or 1) < 0.9
                ) if ht_differential is not None else None,
                "wage_ratio_trend": round(float(slope_wr), 5) if slope_wr is not None else None,
                "convergence": slope_wr > 0 if slope_wr is not None else None,
            }
        results["harris_todaro"] = ht_result or {"error": "no wage differential data"}

        # --- Push-pull factor decomposition ---
        push_pull_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('RURAL_PUSH_INDEX', 'URBAN_PULL_INDEX',
                                   'MIGRATION_PUSH_PULL')
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        push_pull_result = None
        if push_pull_rows:
            meta = json.loads(push_pull_rows[0]["metadata"]) if push_pull_rows[0].get("metadata") else {}
            composite = float(push_pull_rows[0]["value"])

            push_components = {
                "ag_productivity_index": meta.get("ag_productivity_index"),
                "land_scarcity_index": meta.get("land_scarcity_index"),
                "poverty_headcount": meta.get("rural_poverty_headcount"),
                "natural_disaster_exposure": meta.get("natural_disaster_exposure"),
            }
            pull_components = {
                "urban_wage_premium": meta.get("urban_wage_premium"),
                "urban_services_access": meta.get("urban_services_access"),
                "network_density_index": meta.get("social_network_density"),
                "urban_job_growth_rate": meta.get("urban_job_growth_rate"),
            }

            push_scores = [float(v) for v in push_components.values() if v is not None]
            pull_scores = [float(v) for v in pull_components.values() if v is not None]

            push_pull_result = {
                "composite_migration_pressure": round(composite, 4),
                "push_index": round(float(np.mean(push_scores)), 4) if push_scores else None,
                "pull_index": round(float(np.mean(pull_scores)), 4) if pull_scores else None,
                "push_components": {k: round(float(v), 4) if v is not None else None for k, v in push_components.items()},
                "pull_components": {k: round(float(v), 4) if v is not None else None for k, v in pull_components.items()},
                "dominant_force": "push" if push_scores and pull_scores and np.mean(push_scores) > np.mean(pull_scores) else "pull",
            }
        results["push_pull_factors"] = push_pull_result or {"error": "no push-pull data"}

        # --- Urbanization speed ---
        urban_pop_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('SP.URB.TOTL.IN.ZS', 'URBAN_POPULATION_SHARE')
            ORDER BY dp.date
            """,
            (country,),
        )

        urbanization_result = None
        if urban_pop_rows and len(urban_pop_rows) >= 5:
            dates = [r["date"] for r in urban_pop_rows]
            urban_shares = np.array([float(r["value"]) for r in urban_pop_rows])
            t_arr = np.arange(len(urban_shares))

            slope_urb, intercept_urb, r_urb, p_urb, _ = sp_stats.linregress(t_arr, urban_shares)

            latest_share = float(urban_shares[-1])
            speed_pct_per_yr = float(slope_urb)  # pp per year

            # Acceleration: second-order trend
            if len(urban_shares) >= 6:
                first_diffs = np.diff(urban_shares)
                accel = float(np.mean(np.diff(first_diffs))) if len(first_diffs) >= 2 else 0.0
            else:
                accel = 0.0

            urbanization_result = {
                "current_urban_share_pct": round(latest_share, 2),
                "urbanization_speed_pp_per_yr": round(speed_pct_per_yr, 4),
                "acceleration": round(accel, 5),
                "r_squared": round(float(r_urb ** 2), 4),
                "p_value": round(float(p_urb), 4),
                "n_years": len(urban_shares),
                "rapid": speed_pct_per_yr > 1.0,
                "pace": (
                    "very_rapid" if speed_pct_per_yr > 2.0
                    else "rapid" if speed_pct_per_yr > 1.0
                    else "moderate" if speed_pct_per_yr > 0.3
                    else "slow"
                ),
            }
        results["urbanization_speed"] = urbanization_result or {"error": "no urbanization data"}

        # --- Informal sector absorption ---
        informal_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('INFORMAL_EMPLOYMENT_SHARE', 'ILO_INFORMAL_SHARE')
            ORDER BY dp.date
            """,
            (country,),
        )

        informal_result = None
        if informal_rows:
            latest_inf = informal_rows[-1]
            informal_share = float(latest_inf["value"])
            meta = json.loads(latest_inf["metadata"]) if latest_inf.get("metadata") else {}

            formal_job_creation_rate = meta.get("formal_job_creation_rate", None)
            migrant_inflow_rate = meta.get("rural_urban_migrant_inflow_rate", None)

            absorption_ratio = None
            if formal_job_creation_rate and migrant_inflow_rate and float(migrant_inflow_rate) > 0:
                absorption_ratio = float(formal_job_creation_rate) / float(migrant_inflow_rate)

            if len(informal_rows) >= 3:
                inf_arr = np.array([float(r["value"]) for r in informal_rows[-10:]])
                t_arr = np.arange(len(inf_arr))
                slope_inf, _, _, _, _ = sp_stats.linregress(t_arr, inf_arr)
            else:
                slope_inf = None

            informal_result = {
                "informal_employment_share": round(informal_share, 4),
                "formal_job_creation_rate": round(float(formal_job_creation_rate), 4) if formal_job_creation_rate is not None else None,
                "migrant_inflow_rate": round(float(migrant_inflow_rate), 4) if migrant_inflow_rate is not None else None,
                "absorption_ratio": round(absorption_ratio, 4) if absorption_ratio is not None else None,
                "lewis_surplus_exhausted": informal_share < 0.20,
                "distress_migration": absorption_ratio < 1.0 if absorption_ratio is not None else None,
                "trend_slope": round(float(slope_inf), 5) if slope_inf is not None else None,
                "informality_rising": slope_inf > 0 if slope_inf is not None else None,
                "level": (
                    "high" if informal_share > 0.60
                    else "medium" if informal_share > 0.35
                    else "low"
                ),
            }
        results["informal_absorption"] = informal_result or {"error": "no informal sector data"}

        # --- Score ---
        score = 25.0

        # Large HT differential -> strong migration incentive
        if ht_result and not ht_result.get("error"):
            if ht_result.get("migration_incentive"):
                diff = ht_result.get("ht_differential", 0) or 0
                rural_w = ht_result.get("rural_wage", 1) or 1
                score += min(20.0, (diff / rural_w) * 30.0)
            if ht_result.get("todaro_paradox_active"):
                score += 10.0

        # Rapid urbanization
        if urbanization_result and not urbanization_result.get("error"):
            if urbanization_result["pace"] == "very_rapid":
                score += 20.0
            elif urbanization_result["pace"] == "rapid":
                score += 12.0
            elif urbanization_result["pace"] == "moderate":
                score += 5.0

        # High informality + distress migration
        if informal_result and not informal_result.get("error"):
            if informal_result["level"] == "high":
                score += 15.0
            elif informal_result["level"] == "medium":
                score += 7.0
            if informal_result.get("distress_migration"):
                score += 10.0

        score = float(np.clip(score, 0.0, 100.0))
        return {"score": round(score, 1), "results": results}
