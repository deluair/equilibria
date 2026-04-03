"""Land value capture (LVC): transit proximity premium, tax increment financing,
betterment levies, and windfall gain estimation.

Key frameworks:

1. Transit proximity premium (Debrezion, Pels & Rietveld 2007):
   Hedonic pricing: ln(P) = alpha + beta*ln(distance) + gamma*X + epsilon
   where X is property and neighbourhood controls.
   Transit premium = beta coefficient (negative: closer = higher price).
   Capitalisation range: 5-15% for rapid transit, 3-8% for bus rapid transit.
   Distance decay function: price premium decays with distance from station.

2. Tax increment financing (TIF):
   Tax increment = assessed_value_after - assessed_value_before
   TIF revenue = tax_increment * tax_rate
   Used to fund infrastructure improvements that enabled the value increase.
   TIF capture rate = TIF_revenue / (total_value_uplift * tax_rate)

3. Betterment levy (development charge, special assessment):
   Levy = beta * value_uplift_per_parcel
   where beta is the sharing parameter (typically 30-50%).
   Revenue potential = sum(levy_i for all affected parcels)
   Net cost after deducting infrastructure investment.

4. Windfall gain estimation:
   Windfall = land_value_post - land_value_pre - cost_increase_due_to_development
   Betterment = infrastructure-attributable share of windfall
   Unearned increment (George 1879): value increase not due to owner effort.

References:
    Debrezion, G., Pels, E. & Rietveld, P. (2007). The Impact of Railway Stations.
        J. Real Estate Finance and Economics, 35(2).
    Medda, F. (2012). Land Value Capture Finance for Urban Investment. JTEP 46(3).
    Smolka, M. (2013). Implementing Value Capture in Latin America. Lincoln Institute.
    George, H. (1879). Progress and Poverty. Appleton.
    OECD (2022). Land Value Capture: Tools to Finance Urban Development.

Sources: OECD, World Bank Land Governance Assessment Framework, local property databases.
"""

from __future__ import annotations

import json

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class LandValueCapture(LayerBase):
    layer_id = "l11"
    name = "Land Value Capture"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        results: dict = {"country": country}

        # --- Transit proximity premium (hedonic) ---
        hedonic_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('TRANSIT_PRICE_PREMIUM', 'TRANSIT_HEDONIC_BETA')
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        hedonic_result = None
        if hedonic_rows and len(hedonic_rows) >= 3:
            premiums = []
            distances = []
            for r in hedonic_rows:
                meta = json.loads(r["metadata"]) if r.get("metadata") else {}
                prem = meta.get("premium_pct")
                dist = meta.get("distance_km")
                if prem is not None and dist is not None:
                    premiums.append(float(prem))
                    distances.append(float(dist))

            if len(premiums) >= 3:
                prem_arr = np.array(premiums)
                dist_arr = np.array(distances)
                mean_premium = float(np.mean(prem_arr))

                # Distance decay: log-linear fit
                log_dist = np.log(np.maximum(dist_arr, 0.01))
                slope_hed, intercept_hed, r_hed, p_hed, _ = sp_stats.linregress(log_dist, prem_arr)

                hedonic_result = {
                    "mean_transit_premium_pct": round(mean_premium, 2),
                    "distance_decay_slope": round(float(slope_hed), 4),
                    "r_squared": round(float(r_hed ** 2), 4),
                    "p_value": round(float(p_hed), 4),
                    "significant": p_hed < 0.05,
                    "n_observations": len(premiums),
                    "premium_type": (
                        "strong" if mean_premium > 10
                        else "moderate" if mean_premium > 5
                        else "weak"
                    ),
                }
            else:
                hedonic_result = {"error": "insufficient hedonic observations"}
        results["transit_premium"] = hedonic_result or {"error": "no hedonic price data"}

        # --- TIF revenue estimation ---
        tif_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('TIF_VALUE_INCREMENT', 'ASSESSED_VALUE_UPLIFT_GDP')
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        tif_result = None
        if tif_rows:
            meta = json.loads(tif_rows[0]["metadata"]) if tif_rows[0].get("metadata") else {}
            value_increment_pct_gdp = float(tif_rows[0]["value"])
            tax_rate = meta.get("effective_property_tax_rate", 0.015)
            tif_capture_rate = meta.get("tif_capture_rate", 0.60)
            n_districts = meta.get("n_tif_districts", None)

            tif_revenue = value_increment_pct_gdp * float(tax_rate) * float(tif_capture_rate)

            tif_result = {
                "value_increment_pct_gdp": round(value_increment_pct_gdp, 2),
                "effective_property_tax_rate": round(float(tax_rate), 4),
                "tif_capture_rate": round(float(tif_capture_rate), 4),
                "estimated_tif_revenue_pct_gdp": round(tif_revenue, 4),
                "n_tif_districts": int(n_districts) if n_districts is not None else None,
            }
        results["tax_increment_financing"] = tif_result or {"error": "no TIF data"}

        # --- Betterment levy potential ---
        betterment_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('BETTERMENT_LEVY_REVENUE', 'DEVELOPMENT_CHARGE_GDP')
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        betterment_result = None
        if betterment_rows:
            meta = json.loads(betterment_rows[0]["metadata"]) if betterment_rows[0].get("metadata") else {}
            levy_revenue_pct_gdp = float(betterment_rows[0]["value"])
            sharing_parameter = meta.get("sharing_parameter", 0.40)
            total_uplift_pct_gdp = meta.get("total_uplift_pct_gdp", None)
            capture_gap = None
            if total_uplift_pct_gdp is not None:
                max_potential = float(total_uplift_pct_gdp) * float(sharing_parameter)
                capture_gap = max_potential - levy_revenue_pct_gdp

            betterment_result = {
                "levy_revenue_pct_gdp": round(levy_revenue_pct_gdp, 4),
                "sharing_parameter": round(float(sharing_parameter), 3),
                "total_uplift_pct_gdp": round(float(total_uplift_pct_gdp), 2) if total_uplift_pct_gdp is not None else None,
                "capture_gap_pct_gdp": round(capture_gap, 4) if capture_gap is not None else None,
                "implementation": meta.get("implementation_status", "unknown"),
            }
        results["betterment_levy"] = betterment_result or {"error": "no betterment levy data"}

        # --- Windfall gain estimation ---
        windfall_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('LAND_VALUE_WINDFALL', 'REZONING_UPLIFT_GDP')
            ORDER BY dp.date
            """,
            (country,),
        )

        windfall_result = None
        if windfall_rows:
            latest_wf = windfall_rows[-1]
            windfall_pct_gdp = float(latest_wf["value"])
            meta = json.loads(latest_wf["metadata"]) if latest_wf.get("metadata") else {}

            unearned_increment_share = meta.get("unearned_increment_share", 0.70)
            captured_share = meta.get("captured_share", None)

            wf_series = [
                {"date": r["date"], "windfall_pct_gdp": round(float(r["value"]), 4)}
                for r in windfall_rows[-8:]
            ]

            if len(windfall_rows) >= 3:
                wf_arr = np.array([float(r["value"]) for r in windfall_rows[-8:]])
                t_arr = np.arange(len(wf_arr))
                slope_wf, _, _, _, _ = sp_stats.linregress(t_arr, wf_arr)
            else:
                slope_wf = None

            windfall_result = {
                "windfall_pct_gdp": round(windfall_pct_gdp, 2),
                "unearned_increment_share": round(float(unearned_increment_share), 3),
                "public_capture_share": round(float(captured_share), 3) if captured_share is not None else None,
                "trend": wf_series,
                "trend_slope": round(float(slope_wf), 5) if slope_wf is not None else None,
                "rising": slope_wf > 0 if slope_wf is not None else None,
            }
        results["windfall_gains"] = windfall_result or {"error": "no windfall data"}

        # --- Score (high windfall + low capture = stress) ---
        score = 30.0

        # Large uncaptured windfall -> concern
        if windfall_result and not windfall_result.get("error"):
            wf = windfall_result["windfall_pct_gdp"]
            captured = windfall_result.get("public_capture_share", 0.3) or 0.3
            uncaptured_gain = wf * (1.0 - captured)
            score += min(25.0, uncaptured_gain * 5.0)
            if windfall_result.get("rising"):
                score += 5.0

        # Weak transit premium capitalization
        if hedonic_result and not hedonic_result.get("error"):
            if hedonic_result.get("premium_type") == "weak":
                score += 10.0

        # Large capture gap in betterment levy
        if betterment_result and not betterment_result.get("error"):
            gap = betterment_result.get("capture_gap_pct_gdp", 0) or 0
            score += min(15.0, gap * 10.0)

        # Absent TIF mechanism
        if tif_result and not tif_result.get("error"):
            tif_rev = tif_result["estimated_tif_revenue_pct_gdp"]
            if tif_rev < 0.01:
                score += 10.0
        else:
            score += 10.0

        score = float(np.clip(score, 0.0, 100.0))
        return {"score": round(score, 1), "results": results}
