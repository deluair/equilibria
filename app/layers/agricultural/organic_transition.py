"""Organic farming transition analysis: premium dynamics, yield gap, and co-benefits.

Models the economics of transitioning from conventional to organic farming:
organic price premiums, yield penalty during conversion, conversion period
costs, and environmental co-benefits (ecosystem services valuation).

Methodology:
    1. Organic premium dynamics:
       P_org = P_conv * (1 + pi(A_org))
       where pi(A_org) decreasing as organic market share grows (premium erosion).
       Estimated from price spread regression: pi = alpha + beta * A_org + e.

    2. Yield gap estimation (meta-regression):
       YG = (Y_conv - Y_org) / Y_conv
       Empirically: YG ~ 19-25% on average (Seufert et al. 2012).
       Varies by crop type, management intensity, and region.

    3. Conversion period costs (3-5 years):
       Net income during conversion = P_conv * Y_org - C_conv
       where C_conv = higher labor + certification costs.
       Payback period = initial conversion cost / steady-state premium income.

    4. Environmental co-benefits (Costanza et al. ecosystem services framework):
       - Biodiversity: delta_species_richness * willingness_to_pay
       - Carbon: delta_SOC_kg_ha * carbon_price_USD
       - Water: delta_leaching_kg_N * social_cost_N_pollution
       Total co-benefit value per hectare.

    Score: low premium + large yield gap + long payback + small co-benefits = high stress.

References:
    Seufert, V., Ramankutty, N. & Foley, J.A. (2012). "Comparing the yields
        of organic and conventional agriculture." Nature, 485, 229-232.
    Willer, H. & Lernoud, J. (2019). "The World of Organic Agriculture."
        FiBL & IFOAM.
    Costanza, R. et al. (1997). "The value of the world's ecosystem services
        and natural capital." Nature, 387, 253-260.
    Crowder, D.W. & Reganold, J.P. (2015). "Financial competitiveness of
        organic agriculture on a global scale." PNAS, 112(24), 7611-7616.
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class OrganicTransition(LayerBase):
    layer_id = "l5"
    name = "Organic Transition"

    # Yield gap parameters by crop group (from Seufert et al. 2012 meta-analysis)
    YIELD_GAP_BY_CROP = {
        "cereals":    {"mean": 0.21, "std": 0.08},
        "vegetables": {"mean": 0.17, "std": 0.10},
        "fruits":     {"mean": 0.10, "std": 0.09},
        "oilseeds":   {"mean": 0.24, "std": 0.10},
        "legumes":    {"mean": 0.11, "std": 0.08},
        "default":    {"mean": 0.19, "std": 0.09},
    }

    # Environmental co-benefit values (USD per ha per year, approximate)
    CO_BENEFIT_VALUES = {
        "biodiversity_uplift_usd_ha": 85.0,      # 5-10% more species richness
        "carbon_sequestration_usd_ha": 45.0,     # 0.3-0.5 t CO2-eq extra per year @ $30/t
        "nitrate_leaching_reduction_usd_ha": 35.0,
        "pesticide_reduction_health_usd_ha": 25.0,
    }

    # Conversion period costs (% premium over conventional costs)
    CONVERSION_COST_PREMIUM = {
        "year_1": 0.20,   # 20% higher costs (new equipment, certification application)
        "year_2": 0.15,
        "year_3": 0.10,
        "steady_state": 0.08,  # 8% higher ongoing organic costs (labor, certification)
    }

    async def compute(self, db, **kwargs) -> dict:
        """Analyze organic transition economics.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            crop_type : str - crop category for yield gap (default default)
            lookback_years : int - price/share history (default 15)
            carbon_price : float - USD/tCO2 for co-benefit valuation (default 30)
        """
        country = kwargs.get("country_iso3", "BGD")
        crop_type = kwargs.get("crop_type", "default")
        lookback = kwargs.get("lookback_years", 15)
        carbon_price = kwargs.get("carbon_price", 30.0)

        if crop_type not in self.YIELD_GAP_BY_CROP:
            crop_type = "default"

        rows = await db.fetch_all(
            """
            SELECT ds.description, dp.date, dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('fao', 'faostat', 'wdi', 'organic', 'prices')
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
        organic_share = self._extract_series(series, ["organic_share", "organic_area", "certified_organic"])
        org_price = self._extract_series(series, ["organic_price", "price_organic"])
        conv_price = self._extract_series(series, ["conventional_price", "producer_price", "farm_gate"])
        yield_organic = self._extract_series(series, ["organic_yield", "yield_org"])
        yield_conv = self._extract_series(series, ["conventional_yield", "crop_yield", "cereal_yield"])
        farm_income = self._extract_series(series, ["farm_income", "agricultural_income"])

        # --- Organic premium dynamics ---
        premium_result = self._premium_dynamics(org_price, conv_price, organic_share)

        # --- Yield gap estimation ---
        yield_gap_result = self._yield_gap(yield_organic, yield_conv, crop_type)

        # --- Conversion economics ---
        conv_economics = self._conversion_economics(
            conv_price, yield_conv, yield_gap_result, premium_result, farm_income
        )

        # --- Environmental co-benefits ---
        co_benefits = self._co_benefits_valuation(carbon_price)

        # --- Score calculation ---
        # Premium component: high and stable premium = low stress
        premium_component = 50.0
        if premium_result and premium_result.get("current_premium_pct") is not None:
            prem = premium_result["current_premium_pct"]
            premium_component = float(np.clip(100.0 - prem * 2.5, 0, 100))

        # Yield gap: large gap = high stress
        gap_component = 50.0
        if yield_gap_result and yield_gap_result.get("yield_gap_pct") is not None:
            yg = yield_gap_result["yield_gap_pct"]
            gap_component = float(np.clip(yg * 3.5, 0, 100))

        # Payback: long payback = high stress
        payback_component = 50.0
        if conv_economics and conv_economics.get("payback_years") is not None:
            pb = conv_economics["payback_years"]
            payback_component = float(np.clip(pb * 8.0, 0, 100))

        # Co-benefit: large co-benefits reduce net stress
        co_benefit_component = 50.0
        if co_benefits and co_benefits.get("total_usd_ha") is not None:
            cb = co_benefits["total_usd_ha"]
            co_benefit_component = float(np.clip(100.0 - cb / 200.0 * 100.0, 0, 100))

        score = float(np.clip(
            0.25 * premium_component + 0.30 * gap_component
            + 0.25 * payback_component + 0.20 * co_benefit_component,
            0, 100,
        ))

        return {
            "score": round(score, 2),
            "country": country,
            "crop_type": crop_type,
            "premium_dynamics": premium_result,
            "yield_gap": yield_gap_result,
            "conversion_economics": conv_economics,
            "environmental_co_benefits": co_benefits,
        }

    @staticmethod
    def _extract_series(series: dict, keywords: list[str]) -> list[float] | None:
        for key, vals in series.items():
            for kw in keywords:
                if kw in key:
                    return [v[1] for v in vals]
        return None

    def _premium_dynamics(
        self,
        org_price: list[float] | None,
        conv_price: list[float] | None,
        organic_share: list[float] | None,
    ) -> dict:
        """Estimate organic price premium and market-share erosion."""
        if org_price and conv_price and len(org_price) >= 3:
            n = min(len(org_price), len(conv_price))
            p_org = np.array(org_price[-n:])
            p_conv = np.array(conv_price[-n:])
            premiums = (p_org - p_conv) / np.maximum(p_conv, 1e-6) * 100.0
            current_prem = float(premiums[-1])
            prem_trend_slope, _, r_val, _, _ = sp_stats.linregress(np.arange(n), premiums)

            # Premium erosion model: regress premium on organic market share
            erosion = None
            if organic_share and len(organic_share) >= n:
                share_arr = np.array(organic_share[-n:])
                X = np.column_stack([np.ones(n), share_arr])
                beta = np.linalg.lstsq(X, premiums, rcond=None)[0]
                erosion = {
                    "beta_share": round(float(beta[1]), 4),
                    "interpretation": "erosion" if beta[1] < 0 else "no_erosion",
                }

            return {
                "current_premium_pct": round(current_prem, 2),
                "mean_premium_pct": round(float(np.mean(premiums)), 2),
                "premium_trend_slope": round(float(prem_trend_slope), 4),
                "r_squared": round(float(r_val ** 2), 4),
                "erosion_dynamics": erosion,
                "premium_stability": "declining" if prem_trend_slope < -0.5 else
                                     "growing" if prem_trend_slope > 0.5 else "stable",
            }
        else:
            # Use literature default: 20-40% organic premium
            default_prem = 29.0  # Crowder & Reganold 2015 global average
            return {
                "current_premium_pct": default_prem,
                "source": "literature_default_crowder_reganold_2015",
                "premium_stability": "unknown",
            }

    def _yield_gap(
        self,
        yield_org: list[float] | None,
        yield_conv: list[float] | None,
        crop_type: str,
    ) -> dict:
        """Estimate yield gap between organic and conventional."""
        if yield_org and yield_conv and len(yield_org) >= 3:
            n = min(len(yield_org), len(yield_conv))
            y_org = np.array(yield_org[-n:])
            y_conv = np.array(yield_conv[-n:])
            gaps = (y_conv - y_org) / np.maximum(y_conv, 1e-6) * 100.0
            current_gap = float(np.mean(gaps))
            return {
                "yield_gap_pct": round(float(current_gap), 2),
                "source": "observed_data",
                "n_observations": n,
                "gap_trend_slope": round(float(sp_stats.linregress(
                    np.arange(n), gaps).slope), 4),
            }
        else:
            params = self.YIELD_GAP_BY_CROP[crop_type]
            return {
                "yield_gap_pct": round(params["mean"] * 100, 2),
                "std_pct": round(params["std"] * 100, 2),
                "source": "seufert_2012_meta_analysis",
                "crop_type": crop_type,
            }

    def _conversion_economics(
        self,
        conv_price: list[float] | None,
        yield_conv: list[float] | None,
        yield_gap_result: dict | None,
        premium_result: dict | None,
        farm_income: list[float] | None,
    ) -> dict:
        """Model financial economics of 3-year conversion period."""
        price = float(conv_price[-1]) if conv_price else 200.0  # USD/t
        yield_base = float(yield_conv[-1]) if yield_conv else 3.0  # t/ha

        gap_pct = 0.19  # default 19% yield gap
        if yield_gap_result and yield_gap_result.get("yield_gap_pct") is not None:
            gap_pct = yield_gap_result["yield_gap_pct"] / 100.0

        premium_pct = 0.29  # default 29% premium
        if premium_result and premium_result.get("current_premium_pct") is not None:
            premium_pct = premium_result["current_premium_pct"] / 100.0

        # Revenue during conversion (no premium yet in year 1-2)
        year_incomes = []
        conv_costs = [
            self.CONVERSION_COST_PREMIUM["year_1"],
            self.CONVERSION_COST_PREMIUM["year_2"],
            self.CONVERSION_COST_PREMIUM["year_3"],
        ]
        # Yield recovers gradually during conversion
        yield_factors = [1 - gap_pct, 1 - gap_pct * 0.7, 1 - gap_pct * 0.5]
        premium_factors = [0.0, 0.5, 1.0]  # partial premium during conversion

        base_income = price * yield_base
        for yr, (yf, pf, cf) in enumerate(zip(yield_factors, premium_factors, conv_costs)):
            y_income = price * (1 + premium_pct * pf) * yield_base * yf
            cost_inc = base_income * cf
            year_incomes.append(y_income - cost_inc - base_income)  # relative to conv baseline

        # Steady-state premium income
        ss_yield = yield_base * (1 - gap_pct * 0.3)   # partial yield recovery long-term
        ss_income = price * (1 + premium_pct) * ss_yield
        ss_premium_income = ss_income - price * yield_base * (1 + self.CONVERSION_COST_PREMIUM["steady_state"])

        # Net present value of transition (3% discount)
        npv = sum(inc / (1.03 ** (yr + 1)) for yr, inc in enumerate(year_incomes))
        # Payback: total conversion losses / steady-state gain
        total_loss = -sum(min(inc, 0) for inc in year_incomes)
        payback = total_loss / max(ss_premium_income, 1e-6) if ss_premium_income > 0 else 99.0

        return {
            "annual_income_delta_usd_ha": [round(float(inc), 2) for inc in year_incomes],
            "steady_state_premium_income_usd_ha": round(float(ss_premium_income), 2),
            "npv_3yr_usd_ha": round(float(npv), 2),
            "payback_years": round(float(min(payback + 3, 30.0)), 1),
            "financially_viable": ss_premium_income > 0 and payback < 10,
        }

    def _co_benefits_valuation(self, carbon_price: float) -> dict:
        """Value environmental co-benefits of organic farming per hectare."""
        values = dict(self.CO_BENEFIT_VALUES)
        # Adjust carbon benefit for actual carbon price
        reference_carbon_price = 30.0
        values["carbon_sequestration_usd_ha"] = (
            values["carbon_sequestration_usd_ha"] * carbon_price / reference_carbon_price
        )
        total = sum(values.values())
        return {
            "total_usd_ha": round(float(total), 2),
            "components": {k: round(float(v), 2) for k, v in values.items()},
            "carbon_price_used": carbon_price,
        }
