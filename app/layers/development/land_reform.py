"""Land reform: distribution, productivity effects, and tenure security.

Four analytical dimensions:

1. Gini coefficient of land distribution: constructed from quintile or
   decile land ownership data (FAO World Census of Agriculture, FAOSTAT).
   Land Gini typically exceeds income Gini by 15-25 pp (Deininger & Squire 1998).
   High land Gini -> concentrated rents, credit access barriers for smallholders.

2. Productivity effects of redistribution: inverse farm size-productivity
   relationship (Berry & Cline 1979): smaller farms achieve higher output
   per hectare due to lower supervision costs and intensive family labor.
   Total factor productivity gain from redistribution estimated at 10-30%
   in simulation studies (Binswanger-Mkhize 2009).

3. Tenure security and investment: farm households with formal titles invest
   more in soil conservation and long-run improvements. Feder et al. (1988)
   Thailand study: title increases land value by ~73%. Besley (1995)
   Ghana study: stronger rights -> more investment. OLS/IV estimates
   from cross-country panel.

4. Inverse farm size-productivity relationship (IFR): test whether
   land productivity (value added per hectare) is negatively correlated
   with farm size across quintile/decile data. IFR holds in most
   developing countries but breaks down with capital-intensive agriculture.

References:
    Berry, R.A. & Cline, W.R. (1979). Agrarian Structure and Productivity
        in Developing Countries. Baltimore: Johns Hopkins University Press.
    Deininger, K. & Squire, L. (1998). New ways of looking at old issues:
        Inequality and growth. Journal of Development Economics 57: 259-287.
    Feder, G., Onchan, T., Chalamwong, Y. & Hongladarom, C. (1988). Land
        policies and farm productivity in Thailand. Johns Hopkins.
    Besley, T. (1995). Property rights and investment incentives: Theory
        and evidence from Ghana. JPE 103(5): 903-937.
    Binswanger-Mkhize, H., Bourguignon, C. & van den Brink, R. (2009).
        Agricultural Land Redistribution. World Bank.

Score: high land Gini + low tenure security + broken IFR -> STRESS.
Land reform enacted with productivity gains confirmed -> STABLE.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class LandReform(LayerBase):
    layer_id = "l4"
    name = "Land Reform & Tenure"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3")

        # Land distribution data
        land_dist_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id IN ('AG.LND.AGRI.ZS', 'AG.LND.ARBL.HA')
              AND dp.value IS NOT NULL
              AND (:country IS NULL OR ds.country_iso3 = :country)
            ORDER BY ds.country_iso3, dp.date DESC
            """,
            {"country": country_iso3},
        )

        # Gini of land from custom source
        land_gini_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value, dp.date
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.source = 'land_distribution'
              AND ds.series_id LIKE '%land_gini%'
              AND dp.value IS NOT NULL
              AND (:country IS NULL OR ds.country_iso3 = :country)
            ORDER BY ds.country_iso3, dp.date DESC
            """,
            {"country": country_iso3},
        )

        # Farm size productivity data
        farm_size_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.source = 'farm_productivity'
              AND dp.value IS NOT NULL
              AND (:country IS NULL OR ds.country_iso3 = :country)
            ORDER BY dp.date DESC
            """,
            {"country": country_iso3},
        )

        # Tenure security index (World Bank LGAF, or proxy from Doing Business land registration)
        tenure_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id IN ('IC.LGL.CRED.XQ', 'IC.REG.PROC')
              AND dp.value IS NOT NULL
              AND (:country IS NULL OR ds.country_iso3 = :country)
            AND dp.date = (
                SELECT MAX(dp2.date) FROM data_points dp2
                WHERE dp2.series_id = ds.id AND dp2.value IS NOT NULL
            )
            """,
            {"country": country_iso3},
        )

        # Income Gini for comparison
        income_gini_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SI.POV.GINI'
              AND dp.value > 0
              AND (:country IS NULL OR ds.country_iso3 = :country)
            AND dp.date = (
                SELECT MAX(dp2.date) FROM data_points dp2
                WHERE dp2.series_id = ds.id AND dp2.value > 0
            )
            """,
            {"country": country_iso3},
        )

        # Build data maps
        land_gini_map = {}
        for r in land_gini_rows:
            if r["country_iso3"] not in land_gini_map:
                land_gini_map[r["country_iso3"]] = float(r["value"])

        income_gini_map = {r["country_iso3"]: float(r["value"]) for r in income_gini_rows}

        # Farm size vs. productivity: test IFR
        import json
        farm_data: dict[str, list[dict]] = {}
        for r in farm_size_rows:
            meta = json.loads(r["metadata"]) if r.get("metadata") else {}
            iso = r["country_iso3"]
            farm_size = meta.get("farm_size_ha")
            productivity = r["value"]
            if farm_size is not None and productivity is not None:
                farm_data.setdefault(iso, []).append({
                    "farm_size": float(farm_size),
                    "productivity": float(productivity),
                })

        # IFR test for target country
        ifr_result = None
        if country_iso3 and country_iso3 in farm_data and len(farm_data[country_iso3]) >= 4:
            fd = farm_data[country_iso3]
            sizes = np.array([d["farm_size"] for d in fd])
            prods = np.array([d["productivity"] for d in fd])
            # Log-log regression: ln(productivity) = a + b*ln(farm_size)
            # IFR: b < 0
            ln_size = np.log(sizes + 1e-6)
            ln_prod = np.log(prods + 1e-6)
            X = np.column_stack([np.ones(len(ln_size)), ln_size])
            beta = np.linalg.lstsq(X, ln_prod, rcond=None)[0]
            resid = ln_prod - X @ beta
            ss_res = np.sum(resid ** 2)
            ss_tot = np.sum((ln_prod - ln_prod.mean()) ** 2)
            r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

            ifr_result = {
                "size_elasticity": round(float(beta[1]), 4),
                "r_squared": round(float(r2), 4),
                "ifr_holds": float(beta[1]) < 0,
                "n_size_classes": len(fd),
                "interpretation": (
                    "inverse farm size relationship confirmed: smaller farms more productive"
                    if float(beta[1]) < -0.05
                    else "weak or no inverse relationship"
                    if float(beta[1]) < 0
                    else "positive relationship: capital-intensive large farms dominate"
                ),
            }

        # Cross-country land Gini statistics
        land_gini_values = list(land_gini_map.values())
        cross_country_stats = None
        if len(land_gini_values) >= 10:
            arr = np.array(land_gini_values)
            cross_country_stats = {
                "n_countries": len(arr),
                "mean_land_gini": round(float(np.mean(arr)), 2),
                "median_land_gini": round(float(np.median(arr)), 2),
                "p75_land_gini": round(float(np.percentile(arr, 75)), 2),
            }

        # Target country analysis
        target_land_gini = land_gini_map.get(country_iso3) if country_iso3 else None
        target_income_gini = income_gini_map.get(country_iso3) if country_iso3 else None

        gini_gap = None
        if target_land_gini is not None and target_income_gini is not None:
            gini_gap = target_land_gini - target_income_gini

        # Tenure proxy: days to register property (lower = better security)
        tenure_vals = [float(r["value"]) for r in tenure_rows if r["country_iso3"] == country_iso3] \
            if country_iso3 else []
        tenure_proxy = float(np.mean(tenure_vals)) if tenure_vals else None

        # Score construction
        score = 30.0  # default moderate

        if target_land_gini is not None:
            # Land Gini > 70: very high concentration
            if target_land_gini > 70:
                score = 75.0
            elif target_land_gini > 60:
                score = 60.0
            elif target_land_gini > 50:
                score = 45.0
            elif target_land_gini > 40:
                score = 30.0
            else:
                score = 20.0
        elif cross_country_stats:
            # Use global median as reference
            score = 40.0  # unknown but assume average

        # Adjust for IFR (broken IFR = large farm concentration blocking productivity)
        if ifr_result and not ifr_result["ifr_holds"]:
            score = min(90.0, score + 15.0)

        # Penalize for low tenure security (high days = low security)
        if tenure_proxy is not None:
            if tenure_proxy > 60:
                score = min(90.0, score + 10.0)
            elif tenure_proxy < 10:
                score = max(5.0, score - 5.0)

        score = max(0.0, min(100.0, score))

        results: dict = {
            "country_iso3": country_iso3,
            "land_distribution": {
                "land_gini": round(target_land_gini, 2) if target_land_gini is not None else None,
                "income_gini": round(target_income_gini, 2) if target_income_gini is not None else None,
                "gini_gap": round(gini_gap, 2) if gini_gap is not None else None,
                "interpretation": (
                    "highly concentrated land ownership, reform pressure high"
                    if target_land_gini is not None and target_land_gini > 65
                    else "moderate concentration" if target_land_gini is not None and target_land_gini > 45
                    else "relatively equitable" if target_land_gini is not None
                    else "data unavailable"
                ),
            },
            "inverse_farm_size_relationship": ifr_result,
            "tenure_security": {
                "proxy_days_to_register": round(tenure_proxy, 1) if tenure_proxy is not None else None,
                "assessment": (
                    "weak tenure security" if tenure_proxy is not None and tenure_proxy > 60
                    else "moderate" if tenure_proxy is not None and tenure_proxy > 20
                    else "strong tenure security" if tenure_proxy is not None
                    else "data unavailable"
                ),
            },
            "cross_country": cross_country_stats,
        }

        return {"score": round(score, 2), "results": results}
