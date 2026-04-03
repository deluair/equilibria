"""Migration economics with spatial equilibrium.

Roy model of migrant self-selection, Borjas immigration surplus, Rosen-Roback
spatial equilibrium, and brain drain estimation.

Roy model (Roy 1951, Borjas 1987):
    Migrants self-select based on comparative advantage. If returns to skill
    are higher in the destination, positive selection (high-skill emigrate).
    If returns are lower, negative selection.

    Selection indicator: (mu_1 - mu_0 - C) / sigma
    where mu_1, mu_0 are mean log earnings at destination/origin, C is
    migration cost, sigma is earnings dispersion.

Borjas immigration surplus (Borjas 1995):
    Net gain to natives from immigration:
    S = (1/2) * e * s^2 * Q_GDP
    where e = labor demand elasticity, s = immigrant share of labor,
    Q_GDP = total GDP. Small (0.1-0.3% of GDP) but positive.

Rosen-Roback spatial equilibrium (Rosen 1979, Roback 1982):
    Workers and firms sort across locations. In equilibrium, utility is
    equalized: high-amenity cities have higher rents and/or lower wages.

    w_r - r_r * h = V_bar + A_r  (indirect utility equalization)

    Amenity value = wage differential - rent differential.

Brain drain estimation:
    Emigration rate of tertiary-educated / overall emigration rate.
    Brain drain ratio > 1 indicates disproportionate skilled emigration.

References:
    Roy, A. (1951). Some Thoughts on the Distribution of Earnings. Oxford EP.
    Borjas, G. (1987). Self-Selection and the Earnings of Immigrants. AER.
    Borjas, G. (1995). The Economic Benefits from Immigration. JEP 9(2).
    Roback, J. (1982). Wages, Rents, and the Quality of Life. JPE 90(6).

Score: negative selection + large brain drain + spatial disequilibrium -> STRESS.
"""

import json

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class MigrationEconomics(LayerBase):
    layer_id = "l11"
    name = "Migration Economics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        # --- Roy model: self-selection ---
        earnings_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'migration_earnings'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        roy_result = None
        if earnings_rows and len(earnings_rows) >= 10:
            origin_earnings = []
            dest_earnings = []
            costs = []

            for row in earnings_rows:
                meta = json.loads(row["metadata"]) if row.get("metadata") else {}
                e_origin = meta.get("earnings_origin")
                e_dest = meta.get("earnings_dest")
                cost = meta.get("migration_cost", 0)
                if e_origin and e_dest and e_origin > 0 and e_dest > 0:
                    origin_earnings.append(np.log(e_origin))
                    dest_earnings.append(np.log(e_dest))
                    costs.append(float(cost))

            if len(origin_earnings) >= 10:
                origin_arr = np.array(origin_earnings)
                dest_arr = np.array(dest_earnings)
                costs_arr = np.array(costs)

                mu_origin = float(np.mean(origin_arr))
                mu_dest = float(np.mean(dest_arr))
                sigma_origin = float(np.std(origin_arr))
                sigma_dest = float(np.std(dest_arr))
                rho = float(np.corrcoef(origin_arr, dest_arr)[0, 1])
                mean_cost = float(np.mean(costs_arr))

                # Selection indicator
                sigma = np.sqrt(sigma_dest ** 2 + sigma_origin ** 2 - 2 * rho * sigma_dest * sigma_origin)
                sigma = max(sigma, 1e-10)
                selection_index = (mu_dest - mu_origin - mean_cost) / sigma

                # Positive selection if destination has higher returns to skill
                # AND correlation is positive
                if sigma_dest > sigma_origin and rho > 0:
                    selection_type = "positive"
                elif sigma_dest < sigma_origin and rho > 0:
                    selection_type = "negative"
                else:
                    selection_type = "intermediate"

                roy_result = {
                    "mu_origin": round(mu_origin, 4),
                    "mu_dest": round(mu_dest, 4),
                    "sigma_origin": round(sigma_origin, 4),
                    "sigma_dest": round(sigma_dest, 4),
                    "correlation": round(rho, 4),
                    "selection_index": round(float(selection_index), 4),
                    "selection_type": selection_type,
                    "mean_migration_cost": round(mean_cost, 2),
                    "n_obs": len(origin_earnings),
                }

        # --- Borjas immigration surplus ---
        surplus_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'immigration_surplus'
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        surplus_result = None
        if surplus_rows:
            meta = json.loads(surplus_rows[0]["metadata"]) if surplus_rows[0].get("metadata") else {}
            elasticity = meta.get("labor_demand_elasticity", -0.3)
            immigrant_share = meta.get("immigrant_share", 0.0)
            gdp = surplus_rows[0]["value"]

            if gdp and gdp > 0 and immigrant_share > 0:
                # S = 0.5 * |e| * s^2 * GDP
                surplus = 0.5 * abs(elasticity) * immigrant_share ** 2 * gdp
                surplus_pct = surplus / gdp * 100

                surplus_result = {
                    "surplus": round(surplus, 2),
                    "surplus_pct_gdp": round(surplus_pct, 4),
                    "labor_demand_elasticity": round(float(elasticity), 3),
                    "immigrant_share": round(float(immigrant_share), 4),
                    "gdp": round(float(gdp), 2),
                }

        # --- Rosen-Roback spatial equilibrium ---
        spatial_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'spatial_equilibrium'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        rosenroback_result = None
        if spatial_rows and len(spatial_rows) >= 5:
            wages = []
            rents = []
            amenities = []

            for row in spatial_rows:
                meta = json.loads(row["metadata"]) if row.get("metadata") else {}
                w = meta.get("wage")
                r = meta.get("rent")
                a = meta.get("amenity_index")
                if w is not None and r is not None:
                    wages.append(float(w))
                    rents.append(float(r))
                    amenities.append(float(a) if a is not None else 0.0)

            if len(wages) >= 5:
                wages_arr = np.array(wages)
                rents_arr = np.array(rents)
                amenities_arr = np.array(amenities)

                # Wage-rent correlation (should be positive in equilibrium)
                wr_corr, wr_pval = stats.pearsonr(wages_arr, rents_arr)

                # Amenity capitalization: regress rent on amenity controlling for wage
                if np.std(amenities_arr) > 1e-10:
                    X_rr = np.column_stack([np.ones(len(wages)), wages_arr, amenities_arr])
                    beta_rr = np.linalg.lstsq(X_rr, rents_arr, rcond=None)[0]
                    amenity_price = float(beta_rr[2])
                else:
                    amenity_price = None

                # Disequilibrium measure: residual variance from wage-rent regression
                X_wr = np.column_stack([np.ones(len(wages)), wages_arr])
                beta_wr = np.linalg.lstsq(X_wr, rents_arr, rcond=None)[0]
                resid_wr = rents_arr - X_wr @ beta_wr
                disequilibrium = float(np.std(resid_wr) / np.mean(rents_arr))

                rosenroback_result = {
                    "wage_rent_correlation": round(float(wr_corr), 4),
                    "wage_rent_pval": round(float(wr_pval), 6),
                    "amenity_price": round(amenity_price, 4) if amenity_price else None,
                    "disequilibrium_index": round(disequilibrium, 4),
                    "n_locations": len(wages),
                }

        # --- Brain drain estimation ---
        brain_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'brain_drain'
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        brain_drain_result = None
        if brain_rows:
            meta = json.loads(brain_rows[0]["metadata"]) if brain_rows[0].get("metadata") else {}
            tertiary_emigration_rate = meta.get("tertiary_emigration_rate")
            overall_emigration_rate = meta.get("overall_emigration_rate")
            total_emigrants = brain_rows[0]["value"]

            if tertiary_emigration_rate and overall_emigration_rate and overall_emigration_rate > 0:
                brain_drain_ratio = tertiary_emigration_rate / overall_emigration_rate

                brain_drain_result = {
                    "tertiary_emigration_rate": round(float(tertiary_emigration_rate), 4),
                    "overall_emigration_rate": round(float(overall_emigration_rate), 4),
                    "brain_drain_ratio": round(float(brain_drain_ratio), 4),
                    "severe_brain_drain": brain_drain_ratio > 2.0,
                    "total_emigrants": round(float(total_emigrants), 0) if total_emigrants else None,
                }

        # --- Score ---
        scores = []

        # Roy model: negative selection is concerning
        if roy_result:
            if roy_result["selection_type"] == "negative":
                scores.append(70.0)
            elif roy_result["selection_type"] == "intermediate":
                scores.append(45.0)
            else:
                scores.append(20.0)

        # Brain drain
        if brain_drain_result:
            ratio = brain_drain_result["brain_drain_ratio"]
            if ratio > 3.0:
                scores.append(85.0)
            elif ratio > 2.0:
                scores.append(65.0)
            elif ratio > 1.5:
                scores.append(45.0)
            else:
                scores.append(20.0)

        # Spatial equilibrium
        if rosenroback_result:
            diseq = rosenroback_result["disequilibrium_index"]
            scores.append(min(90.0, diseq * 200.0))

        # Surplus (small surplus = mild concern)
        if surplus_result:
            s_pct = surplus_result["surplus_pct_gdp"]
            if s_pct < 0.05:
                scores.append(60.0)
            elif s_pct < 0.2:
                scores.append(35.0)
            else:
                scores.append(20.0)

        score = float(np.mean(scores)) if scores else 50.0
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "roy_model": roy_result,
            "borjas_surplus": surplus_result,
            "rosen_roback": rosenroback_result,
            "brain_drain": brain_drain_result,
        }
