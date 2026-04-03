"""Returns to irrigation investment analysis.

Quantifies the economic returns to irrigation by comparing irrigated vs
rainfed yields, computing water use efficiency metrics, and estimating
the marginal returns to irrigation investment.

Methodology:
    1. Yield gap estimation: difference between irrigated and rainfed yields
       controlling for other inputs (soil quality, fertilizer, labor).

       ln(y_i) = alpha + beta*IRRIG_i + gamma*X_i + e_i

       The irrigation premium beta captures causal yield impact.

    2. Water use efficiency (WUE):
       - Crop WUE = yield (kg) / water applied (m3)
       - Economic WUE = revenue ($) / water applied (m3)
       - Irrigation efficiency = beneficial water use / total water applied

    3. Economic water productivity (EWP):
       EWP = (revenue_irrigated - revenue_rainfed) / water_applied

    4. Investment returns:
       - NPV of irrigation infrastructure over expected lifespan
       - Marginal return per m3 of water
       - Break-even water price

    Score reflects yield gap exploitation: large unexploited gaps indicate
    high potential returns (moderate score), while already-high irrigation
    coverage with poor WUE indicates stress.

References:
    Dillon, A. (2011). "The effect of irrigation on poverty reduction,
        asset accumulation, and informal insurance." World Development.
    Molden, D. et al. (2010). "Improving agricultural water productivity:
        Between optimism and caution." Agricultural Water Management.
    Giordano, M. et al. (2012). "Water for wealth and food security."
        Water Policy, 14(S1), 24-42.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class IrrigationReturns(LayerBase):
    layer_id = "l5"
    name = "Irrigation Returns"

    async def compute(self, db, **kwargs) -> dict:
        """Compute irrigation returns and water productivity metrics.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            crop : str - crop filter
            infrastructure_cost_per_ha : float - capital cost (default from data)
            lifespan_years : int - infrastructure lifespan (default 20)
            discount_rate : float - default 0.08
        """
        country = kwargs.get("country_iso3", "BGD")
        crop = kwargs.get("crop")
        infra_cost = kwargs.get("infrastructure_cost_per_ha")
        lifespan = kwargs.get("lifespan_years", 20)
        discount_rate = kwargs.get("discount_rate", 0.08)

        crop_clause = "AND ds.description LIKE '%' || ? || '%'" if crop else ""
        params = [country]
        if crop:
            params.append(crop)

        rows = await db.fetch_all(
            f"""
            SELECT dp.value AS yield_val, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'irrigation_data'
              AND ds.country_iso3 = ?
              {crop_clause}
            ORDER BY dp.date
            """,
            tuple(params),
        )

        if not rows or len(rows) < 10:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient irrigation data"}

        import json

        yields_irrig = []
        yields_rain = []
        water_applied = []  # m3/ha
        crop_prices = []
        fert_levels = []
        labor_levels = []
        soil_quality = []

        for row in rows:
            y_val = row["yield_val"]
            if y_val is None or y_val <= 0:
                continue
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            is_irrigated = meta.get("irrigated", False)
            water_m3 = meta.get("water_applied_m3_ha", 0)

            if is_irrigated:
                yields_irrig.append(float(y_val))
                water_applied.append(float(water_m3))
            else:
                yields_rain.append(float(y_val))

            if meta.get("crop_price"):
                crop_prices.append(float(meta["crop_price"]))
            fert_levels.append(float(meta.get("fertilizer_kg_ha", 0)))
            labor_levels.append(float(meta.get("labor_days_ha", 0)))
            soil_quality.append(float(meta.get("soil_quality_index", 50)))

        if len(yields_irrig) < 3 or len(yields_rain) < 3:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "need both irrigated and rainfed observations"}

        y_irrig = np.array(yields_irrig)
        y_rain = np.array(yields_rain)
        crop_price = float(np.median(crop_prices)) if crop_prices else 0.3

        # Yield gap
        mean_irrig = float(y_irrig.mean())
        mean_rain = float(y_rain.mean())
        yield_gap = mean_irrig - mean_rain
        yield_gap_pct = (yield_gap / mean_rain * 100) if mean_rain > 0 else 0.0

        # Regression-based irrigation premium (controlling for inputs)
        # Combine all observations with irrigation dummy
        all_yields = np.concatenate([y_irrig, y_rain])
        irrig_dummy = np.concatenate([np.ones(len(y_irrig)), np.zeros(len(y_rain))])
        n_total = len(all_yields)

        ln_y = np.log(all_yields)
        X = np.column_stack([np.ones(n_total), irrig_dummy])

        # Add controls if available
        if len(fert_levels) == n_total:
            fert_arr = np.array(fert_levels)
            if fert_arr.std() > 0:
                X = np.column_stack([X, fert_arr])

        beta = np.linalg.lstsq(X, ln_y, rcond=None)[0]
        irrig_premium_log = float(beta[1])
        irrig_premium_pct = (np.exp(irrig_premium_log) - 1) * 100

        # HC1 standard errors
        fitted = X @ beta
        resid = ln_y - fitted
        n_k = n_total - X.shape[1]
        XtX_inv = np.linalg.pinv(X.T @ X)
        scale = n_total / max(n_k, 1)
        V = XtX_inv @ (X.T @ np.diag(resid ** 2 * scale) @ X) @ XtX_inv
        se = np.sqrt(np.maximum(np.diag(V), 0.0))
        t_stat = beta[1] / se[1] if se[1] > 0 else 0.0

        ss_res = float(np.sum(resid ** 2))
        ss_tot = float(np.sum((ln_y - ln_y.mean()) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        # Water use efficiency
        w_arr = np.array(water_applied) if water_applied else np.array([0.0])
        mean_water = float(w_arr.mean()) if w_arr.sum() > 0 else 0.0
        crop_wue = mean_irrig / mean_water if mean_water > 0 else 0.0  # kg/m3
        econ_wue = (mean_irrig * crop_price) / mean_water if mean_water > 0 else 0.0  # $/m3

        # Economic water productivity
        revenue_gain = yield_gap * crop_price  # $/ha
        ewp = revenue_gain / mean_water if mean_water > 0 else 0.0  # $/m3

        # Investment NPV
        if infra_cost is None:
            infra_cost = 2000.0  # default $/ha for surface irrigation

        annual_benefit = revenue_gain
        annual_om_cost = infra_cost * 0.03  # 3% of capital for O&M
        net_annual = annual_benefit - annual_om_cost

        t = np.arange(lifespan + 1)
        cash_flows = np.zeros(lifespan + 1)
        cash_flows[0] = -infra_cost
        cash_flows[1:] = net_annual
        discount_factors = (1 + discount_rate) ** (-t)
        npv = float(np.sum(cash_flows * discount_factors))
        pv_benefits = float(np.sum(np.maximum(cash_flows, 0) * discount_factors))
        pv_costs = float(np.sum(np.abs(np.minimum(cash_flows, 0)) * discount_factors))
        bcr = pv_benefits / pv_costs if pv_costs > 0 else 0.0

        # Break-even water price
        total_water_over_life = mean_water * lifespan
        breakeven_water_price = infra_cost / total_water_over_life if total_water_over_life > 0 else float("inf")

        # Score: large unexploited yield gap with low irrigation = moderate stress
        # Poor WUE with high irrigation = high stress
        # Good WUE with full irrigation = low stress
        irrig_coverage = len(yields_irrig) / n_total  # proxy
        if irrig_coverage < 0.3:
            # Low coverage, score based on yield gap potential
            score = float(np.clip(yield_gap_pct / 2.0, 20, 70))
        elif crop_wue < 0.5:
            # High coverage but poor efficiency
            score = float(np.clip(80 - crop_wue * 40, 40, 90))
        else:
            # Good coverage and efficiency
            score = float(np.clip(30 - bcr * 5, 0, 40))
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_irrigated": len(yields_irrig),
            "n_rainfed": len(yields_rain),
            "yield_gap": {
                "irrigated_mean_kg_ha": round(mean_irrig, 1),
                "rainfed_mean_kg_ha": round(mean_rain, 1),
                "gap_kg_ha": round(yield_gap, 1),
                "gap_pct": round(yield_gap_pct, 1),
            },
            "irrigation_premium": {
                "log_coefficient": round(irrig_premium_log, 4),
                "pct_increase": round(float(irrig_premium_pct), 2),
                "t_statistic": round(t_stat, 2),
                "r_squared": round(r2, 4),
            },
            "water_productivity": {
                "mean_water_applied_m3_ha": round(mean_water, 0),
                "crop_wue_kg_per_m3": round(crop_wue, 3),
                "economic_wue_usd_per_m3": round(econ_wue, 4),
                "ewp_usd_per_m3": round(ewp, 4),
            },
            "investment_analysis": {
                "infrastructure_cost_per_ha": round(infra_cost, 0),
                "annual_net_benefit_per_ha": round(net_annual, 2),
                "npv_per_ha": round(npv, 2),
                "bcr": round(bcr, 3),
                "breakeven_water_price_usd_m3": round(breakeven_water_price, 4),
            },
        }
