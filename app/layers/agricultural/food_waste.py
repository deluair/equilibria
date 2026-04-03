"""Food loss and waste analysis: FAO food loss index, economic cost, and supply chain decomposition.

Estimates food waste using the FAO Food Loss Index (FLI), decomposes losses
by supply chain stage, calculates economic cost, and evaluates policy
intervention cost-effectiveness.

Methodology:
    1. FAO Food Loss Index:
       FLI_c = 100 * (sum_k w_k * L_ck) / (sum_k w_k * L_0k)
       where L_ck = loss rate for commodity k in country c,
       L_0k = reference loss rate (baseline period 2014-16),
       w_k = production weight.

    2. Economic cost of waste:
       C_waste = sum_{s,k} Q_k * L_{sk} * P_k * (1 + theta_s)
       where s = supply chain stage, Q_k = production volume,
       P_k = farm-gate price, theta_s = value-added markup at stage s.

    3. Supply chain stage decomposition:
       Stages: farm (harvest), post-harvest (handling/storage),
               processing, distribution, retail, consumer.
       Loss at each stage estimated from FAO SAVE FOOD data.

    4. Policy intervention cost-effectiveness (cost-benefit):
       CBA_i = (Q_saved_i * P_k) / C_intervention_i
       where i = intervention type (cold chain, packaging, etc.)

    Score: high FLI + large economic loss + late-stage waste = high stress.

References:
    FAO (2019). "The State of Food and Agriculture 2019. Moving forward on
        food loss and waste reduction." Rome.
    Gustavsson, J. et al. (2011). "Global food losses and food waste."
        FAO, Rome.
    Buzby, J.C. & Hyman, J. (2012). "Total and per capita value of food loss
        in the United States." Food Policy, 37(5), 561-570.
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class FoodWaste(LayerBase):
    layer_id = "l5"
    name = "Food Waste"

    # FAO reference loss rates by supply chain stage (% of production weight)
    # Source: FAO SAVE FOOD initiative / Gustavsson et al. 2011
    STAGE_LOSS_RATES = {
        "farm_harvest":          {"low_income": 9.0,  "middle_income": 5.0,  "high_income": 2.0},
        "post_harvest_storage":  {"low_income": 8.0,  "middle_income": 5.0,  "high_income": 1.0},
        "processing":            {"low_income": 4.0,  "middle_income": 3.0,  "high_income": 2.0},
        "distribution":          {"low_income": 3.0,  "middle_income": 3.0,  "high_income": 3.0},
        "retail":                {"low_income": 2.0,  "middle_income": 3.0,  "high_income": 5.0},
        "consumer":              {"low_income": 5.0,  "middle_income": 10.0, "high_income": 19.0},
    }

    # Value-added markup by stage (fraction above farm-gate price)
    STAGE_MARKUP = {
        "farm_harvest": 0.0,
        "post_harvest_storage": 0.10,
        "processing": 0.35,
        "distribution": 0.55,
        "retail": 0.90,
        "consumer": 1.20,
    }

    # Intervention types with typical cost per ton saved (USD, approximate)
    INTERVENTION_COSTS = {
        "cold_chain": {"cost_per_ton": 40.0, "stage": "post_harvest_storage", "efficacy": 0.40},
        "improved_packaging": {"cost_per_ton": 15.0, "stage": "distribution", "efficacy": 0.25},
        "retailer_standards": {"cost_per_ton": 10.0, "stage": "retail", "efficacy": 0.30},
        "consumer_campaigns": {"cost_per_ton": 5.0, "stage": "consumer", "efficacy": 0.15},
        "harvest_mechanization": {"cost_per_ton": 30.0, "stage": "farm_harvest", "efficacy": 0.35},
    }

    async def compute(self, db, **kwargs) -> dict:
        """Compute food loss and waste metrics.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            income_level : str - low_income / middle_income / high_income (default middle_income)
            lookback_years : int - trend window (default 10)
        """
        country = kwargs.get("country_iso3", "BGD")
        income_level = kwargs.get("income_level", "middle_income")
        lookback = kwargs.get("lookback_years", 10)

        if income_level not in ("low_income", "middle_income", "high_income"):
            income_level = "middle_income"

        rows = await db.fetch_all(
            """
            SELECT ds.description, dp.date, dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('fao', 'faostat', 'wdi', 'food_waste')
              AND ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
            ORDER BY ds.description, dp.date
            """,
            (country, f"-{lookback} years"),
        )

        series: dict[str, list[tuple[str, float]]] = {}
        for r in rows:
            desc = (r["description"] or "").lower()
            series.setdefault(desc, []).append((r["date"], float(r["value"])))

        # Extract series
        food_loss = self._extract_series(series, ["food_loss", "food_waste", "loss_index"])
        production = self._extract_series(series, ["food_production", "cereal_production", "agricultural_production"])
        farm_price = self._extract_series(series, ["producer_price", "farm_gate_price", "commodity_price"])
        gdp_per_cap = self._extract_series(series, ["gdp_per_capita", "gdp_pc"])

        # Determine income level from GDP per capita if available
        if gdp_per_cap:
            latest_gdp = float(gdp_per_cap[-1])
            if latest_gdp < 1500:
                income_level = "low_income"
            elif latest_gdp < 12000:
                income_level = "middle_income"
            else:
                income_level = "high_income"

        # --- FAO Food Loss Index ---
        fli_result = self._compute_fli(food_loss, income_level)

        # --- Supply chain decomposition ---
        stage_decomp = self._supply_chain_decomposition(income_level)

        # --- Economic cost of waste ---
        econ_cost = self._economic_cost(production, farm_price, stage_decomp)

        # --- Policy intervention CBA ---
        cba_result = self._intervention_cba(production, farm_price, income_level)

        # --- FLI trend ---
        fli_trend = None
        if food_loss and len(food_loss) >= 4:
            fl_arr = np.array(food_loss)
            slope, _, r_val, _, _ = sp_stats.linregress(np.arange(len(fl_arr)), fl_arr)
            fli_trend = {
                "slope_per_year": round(float(slope), 4),
                "r_squared": round(float(r_val ** 2), 4),
                "direction": "improving" if slope < -0.1 else "worsening" if slope > 0.1 else "stable",
            }

        # --- Score ---
        # FLI component: high loss index = high score
        fli_component = 50.0
        if fli_result and fli_result.get("fli") is not None:
            fli_val = fli_result["fli"]
            fli_component = float(np.clip(fli_val, 0, 100))

        # Economic cost as % of agricultural GDP (high cost = high stress)
        econ_component = 50.0
        if econ_cost and econ_cost.get("total_loss_value") is not None:
            # Normalize against a rough agricultural GDP proxy
            loss_val = econ_cost["total_loss_value"]
            prod_val = econ_cost.get("total_production_value", loss_val * 5.0)
            loss_share = loss_val / max(prod_val, 1e-6)
            econ_component = float(np.clip(loss_share * 500.0, 0, 100))

        # Stage distribution: consumer-stage waste = higher score (harder to recapture)
        stage_component = 50.0
        if stage_decomp:
            consumer_share = stage_decomp.get("consumer", {}).get("share_of_total", 0)
            stage_component = float(np.clip(consumer_share * 200.0, 0, 100))

        score = float(np.clip(
            0.40 * fli_component + 0.35 * econ_component + 0.25 * stage_component,
            0, 100,
        ))

        return {
            "score": round(score, 2),
            "country": country,
            "income_level": income_level,
            "food_loss_index": fli_result,
            "supply_chain_decomposition": stage_decomp,
            "economic_cost": econ_cost,
            "intervention_cba": cba_result,
            "fli_trend": fli_trend,
        }

    @staticmethod
    def _extract_series(series: dict, keywords: list[str]) -> list[float] | None:
        for key, vals in series.items():
            for kw in keywords:
                if kw in key:
                    return [v[1] for v in vals]
        return None

    def _compute_fli(
        self, food_loss: list[float] | None, income_level: str
    ) -> dict:
        """Compute FAO-style Food Loss Index relative to reference period."""
        # Reference loss rate: total across all stages for income level
        ref_total = sum(v[income_level] for v in self.STAGE_LOSS_RATES.values())

        if food_loss and len(food_loss) >= 1:
            current = float(food_loss[-1])
            # If data is already an index (0-100), normalize directly
            if current <= 100:
                fli = current
            else:
                fli = current / ref_total * 100.0
        else:
            # Use income-level default
            fli = ref_total / 35.0 * 100.0  # normalize: world avg ~35% total losses

        return {
            "fli": round(float(np.clip(fli, 0, 200)), 2),
            "reference_total_loss_pct": round(float(ref_total), 2),
            "income_group_losses_pct": round(float(ref_total), 2),
            "interpretation": (
                "below_reference" if fli < 90
                else "above_reference" if fli > 110
                else "near_reference"
            ),
        }

    def _supply_chain_decomposition(self, income_level: str) -> dict:
        """Decompose total food loss by supply chain stage."""
        stage_losses = {
            stage: rates[income_level]
            for stage, rates in self.STAGE_LOSS_RATES.items()
        }
        total_loss = sum(stage_losses.values())
        result = {}
        for stage, loss_pct in stage_losses.items():
            share = loss_pct / max(total_loss, 1e-6)
            result[stage] = {
                "loss_rate_pct": round(loss_pct, 2),
                "share_of_total": round(float(share), 4),
                "value_multiplier": 1.0 + self.STAGE_MARKUP[stage],
            }
        result["total_loss_rate_pct"] = round(total_loss, 2)
        return result

    @staticmethod
    def _economic_cost(
        production: list[float] | None,
        farm_price: list[float] | None,
        stage_decomp: dict,
    ) -> dict | None:
        """Compute total economic cost of food waste across supply chain stages."""
        if not production:
            return None

        prod_vol = float(production[-1])
        price = float(farm_price[-1]) if farm_price else 200.0  # default USD/t

        total_value = prod_vol * price
        total_loss_pct = stage_decomp.get("total_loss_rate_pct", 30.0)

        # Cost at each stage accounts for value added
        stage_costs = {}
        total_loss_value = 0.0
        for stage, stage_data in stage_decomp.items():
            if not isinstance(stage_data, dict):
                continue
            loss_pct = stage_data["loss_rate_pct"]
            markup = stage_data["value_multiplier"]
            cost = prod_vol * (loss_pct / 100.0) * price * markup
            stage_costs[stage] = round(float(cost), 2)
            total_loss_value += cost

        return {
            "total_production_value": round(float(total_value), 2),
            "total_loss_value": round(float(total_loss_value), 2),
            "loss_as_pct_production_value": round(float(total_loss_value / max(total_value, 1e-6) * 100), 2),
            "stage_costs": stage_costs,
            "unit": "USD (production-weighted)",
        }

    def _intervention_cba(
        self,
        production: list[float] | None,
        farm_price: list[float] | None,
        income_level: str,
    ) -> dict:
        """Cost-benefit analysis for key food waste interventions."""
        prod_vol = float(production[-1]) if production else 1000.0
        price = float(farm_price[-1]) if farm_price else 200.0

        results = {}
        for name, params in self.INTERVENTION_COSTS.items():
            stage = params["stage"]
            base_loss_pct = self.STAGE_LOSS_RATES[stage][income_level] / 100.0
            efficacy = params["efficacy"]
            cost_per_ton = params["cost_per_ton"]

            # Quantity saved
            q_saved = prod_vol * base_loss_pct * efficacy
            # Value saved (at stage market value)
            markup = 1.0 + self.STAGE_MARKUP[stage]
            value_saved = q_saved * price * markup
            # Intervention cost
            intervention_cost = prod_vol * base_loss_pct * efficacy * cost_per_ton
            bcr = value_saved / max(intervention_cost, 1e-6)

            results[name] = {
                "tonnes_saved": round(float(q_saved), 2),
                "value_saved_usd": round(float(value_saved), 2),
                "intervention_cost_usd": round(float(intervention_cost), 2),
                "benefit_cost_ratio": round(float(bcr), 3),
                "cost_effective": bcr > 1.5,
            }

        # Rank by BCR
        ranked = sorted(results.items(), key=lambda x: x[1]["benefit_cost_ratio"], reverse=True)
        return {
            "interventions": results,
            "most_cost_effective": ranked[0][0] if ranked else None,
        }
