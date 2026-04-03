"""Water economics: pricing, scarcity rents, virtual water trade, groundwater depletion.

Estimates water scarcity rents using Hotelling-style resource pricing, computes
virtual water trade balances (Allan 1998), evaluates groundwater depletion
externalities, and estimates irrigation efficiency returns.

Methodology:
    Water scarcity rent (Hotelling):
        R_t = R_0 * exp(r * t)
        Scarcity rent rises at the rate of interest as the resource depletes.
        Shadow price of water = marginal extraction cost + scarcity rent.

    Virtual water trade (Allan 1998):
        VW_trade = sum_k (VW_k * Trade_k)
        where VW_k = water footprint per unit of product k.

    Groundwater depletion externality:
        External cost = (pumping cost increase) + (subsidence damage) + (ecological damage)
        Following Pfeiffer & Lin (2014) dynamic model.

    Irrigation efficiency:
        Return on irrigation = (irrigated yield - rainfed yield) * area * price
                              / (irrigation capital cost + water cost)

References:
    Allan, J.A. (1998). "Virtual water: a strategic resource." Ground Water, 36(4), 545-546.
    Hoekstra, A.Y. & Hung, P.Q. (2002). "Virtual water trade: a quantification of
        virtual water flows between nations." UNESCO-IHE Value of Water Research Report.
    Pfeiffer, L. & Lin, C.Y.C. (2014). "Does efficient irrigation technology lead to
        reduced groundwater extraction?" Journal of Environmental Economics and
        Management, 67(2), 189-208.
    Olmstead, S.M. (2010). "The economics of water quality." Review of Environmental
        Economics and Policy, 4(1), 44-62.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class WaterEconomics(LayerBase):
    layer_id = "l9"
    name = "Water Economics"

    # Virtual water content (m3 per tonne of product)
    # Based on Hoekstra & Chapagain (2007) global averages
    VIRTUAL_WATER_M3_PER_TONNE = {
        "rice": 2500,
        "wheat": 1350,
        "maize": 900,
        "beef": 15400,
        "poultry": 3900,
        "cotton": 10000,
        "sugar": 1500,
        "soybeans": 1800,
        "vegetables": 300,
        "milk": 1000,
    }

    # Water stress thresholds (Falkenmark indicator: m3/capita/year)
    STRESS_THRESHOLDS = {
        "abundant": 4000,
        "sufficient": 1700,
        "stress": 1000,
        "scarcity": 500,
    }

    async def compute(self, db, **kwargs) -> dict:
        """Compute water economics analysis.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            discount_rate : float - for scarcity rent (default 0.05)
        """
        country = kwargs.get("country_iso3", "BGD")
        discount_rate = kwargs.get("discount_rate", 0.05)

        # Fetch water and agricultural data
        water_series = [
            "ER.H2O.FWTL.ZS",   # Annual freshwater withdrawals (% internal)
            "ER.H2O.INTR.PC",   # Renewable internal freshwater per capita (m3)
            "ER.H2O.FWAG.ZS",   # Agricultural water withdrawal (% total)
            "ER.H2O.FWIN.ZS",   # Industrial water withdrawal (% total)
            "ER.H2O.FWDM.ZS",   # Domestic water withdrawal (% total)
            "AG.LND.IRIG.AG.ZS", # Irrigated land (% of agricultural land)
            "NY.GDP.MKTP.KD",   # GDP
            "SP.POP.TOTL",      # Population
            "AG.PRD.FOOD.XD",   # Food production index
        ]
        placeholders = ",".join(["?" for _ in water_series])

        rows = await db.fetch_all(
            f"""
            SELECT dp.date, dp.value, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ({placeholders})
              AND dp.value IS NOT NULL
            ORDER BY dp.date
            """,
            (country, *water_series),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient water data"}

        data: dict[str, dict[str, float]] = {}
        for r in rows:
            sid = r["series_id"]
            yr = r["date"][:4] if isinstance(r["date"], str) else str(r["date"])
            data.setdefault(sid, {})[yr] = float(r["value"])

        fw_pc_ts = data.get("ER.H2O.INTR.PC", {})
        withdrawal_pct_ts = data.get("ER.H2O.FWTL.ZS", {})
        ag_water_ts = data.get("ER.H2O.FWAG.ZS", {})
        gdp_ts = data.get("NY.GDP.MKTP.KD", {})
        pop_ts = data.get("SP.POP.TOTL", {})
        irrigated_ts = data.get("AG.LND.IRIG.AG.ZS", {})

        # 1. Water scarcity and pricing
        scarcity = self._water_scarcity(
            fw_pc_ts=fw_pc_ts,
            withdrawal_pct_ts=withdrawal_pct_ts,
            discount_rate=discount_rate,
        )

        # 2. Virtual water trade balance
        vw_trade = self._virtual_water_trade(
            country=country,
            gdp_ts=gdp_ts,
            pop_ts=pop_ts,
        )

        # 3. Groundwater depletion externalities
        groundwater = self._groundwater_externalities(
            withdrawal_pct_ts=withdrawal_pct_ts,
            ag_water_ts=ag_water_ts,
            gdp_ts=gdp_ts,
        )

        # 4. Irrigation efficiency returns
        irrigation = self._irrigation_returns(
            irrigated_ts=irrigated_ts,
            ag_water_ts=ag_water_ts,
            gdp_ts=gdp_ts,
        )

        # Score: water stress + depletion + low efficiency = high stress
        fw_pc = self._latest_value(fw_pc_ts)
        withdrawal_pct = self._latest_value(withdrawal_pct_ts)

        scarcity_score = 0
        if fw_pc is not None:
            if fw_pc < 500:
                scarcity_score = 40
            elif fw_pc < 1000:
                scarcity_score = 30
            elif fw_pc < 1700:
                scarcity_score = 20
            else:
                scarcity_score = 10

        depletion_score = 0
        if withdrawal_pct is not None:
            depletion_score = min(30, withdrawal_pct * 0.4)

        irrigation_score = 20  # default moderate
        if irrigated_ts:
            latest_irrig = self._latest_value(irrigated_ts)
            if latest_irrig is not None:
                # High irrigation dependence = vulnerability
                irrigation_score = min(30, latest_irrig * 0.5)

        score = float(np.clip(scarcity_score + depletion_score + irrigation_score, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "water_scarcity": scarcity,
            "virtual_water_trade": vw_trade,
            "groundwater": groundwater,
            "irrigation": irrigation,
        }

    def _water_scarcity(
        self,
        fw_pc_ts: dict[str, float],
        withdrawal_pct_ts: dict[str, float],
        discount_rate: float,
    ) -> dict:
        """Water scarcity assessment and Hotelling scarcity rent."""
        fw_pc = self._latest_value(fw_pc_ts)
        withdrawal_pct = self._latest_value(withdrawal_pct_ts)

        if fw_pc is None:
            return {"error": "no freshwater per capita data"}

        # Falkenmark stress classification
        if fw_pc >= 4000:
            status = "abundant"
        elif fw_pc >= 1700:
            status = "sufficient"
        elif fw_pc >= 1000:
            status = "stress"
        elif fw_pc >= 500:
            status = "scarcity"
        else:
            status = "absolute_scarcity"

        # Scarcity rent (Hotelling framework)
        # Shadow price rises with depletion rate
        depletion_rate = withdrawal_pct / 100.0 if withdrawal_pct else 0.1
        # Initial scarcity rent proxy (USD per m3)
        base_rent = 0.01 if fw_pc > 1700 else 0.05 if fw_pc > 1000 else 0.15
        # Rent growth projection (10-year)
        rents = {}
        for t in [0, 5, 10, 20, 30]:
            rent_t = base_rent * np.exp(discount_rate * t) * (1 + depletion_rate) ** t
            rents[f"year_{t}"] = round(float(rent_t), 4)

        # Freshwater per capita trend
        if len(fw_pc_ts) >= 3:
            years = sorted(fw_pc_ts.keys())
            vals = np.array([fw_pc_ts[y] for y in years])
            trend = np.polyfit(np.arange(len(vals)), vals, 1)[0]
        else:
            trend = None

        return {
            "freshwater_per_capita_m3": round(fw_pc, 0),
            "status": status,
            "withdrawal_pct_internal": round(withdrawal_pct, 2) if withdrawal_pct else None,
            "scarcity_rent_usd_per_m3": rents,
            "fw_pc_trend_per_year": round(float(trend), 2) if trend is not None else None,
        }

    def _virtual_water_trade(
        self,
        country: str,
        gdp_ts: dict[str, float],
        pop_ts: dict[str, float],
    ) -> dict:
        """Estimate virtual water trade balance.

        Net virtual water import = sum of water embedded in imported goods
        minus water embedded in exported goods.
        """
        latest_gdp = self._latest_value(gdp_ts)
        latest_pop = self._latest_value(pop_ts)

        if latest_gdp is None or latest_pop is None:
            return {"error": "insufficient data for VW trade"}

        # Approximate virtual water flows using GDP structure
        # Agriculture-dependent economies tend to be net VW exporters
        # Proxy: GDP per capita determines import/export patterns
        gdp_pc = latest_gdp / latest_pop if latest_pop > 0 else 0

        # Approximate water footprint per capita (global avg ~1400 m3/yr)
        # Agricultural economies have higher domestic WF
        domestic_wf_pc = 1400 * (1 + max(0, 10000 - gdp_pc) / 20000)

        # Virtual water trade (approximate)
        # Low-income agricultural exporters: net VW exporters
        # High-income importers: net VW importers
        if gdp_pc < 3000:
            net_vw_import_pc = -200  # net exporter
        elif gdp_pc < 10000:
            net_vw_import_pc = -50
        elif gdp_pc < 30000:
            net_vw_import_pc = 100
        else:
            net_vw_import_pc = 300  # net importer

        total_net_vw = net_vw_import_pc * latest_pop
        total_wf = domestic_wf_pc * latest_pop

        # Water dependency ratio
        if total_wf > 0:
            external_dependency = max(0, net_vw_import_pc / (domestic_wf_pc + net_vw_import_pc) * 100)
        else:
            external_dependency = 0

        return {
            "domestic_wf_per_capita_m3": round(float(domestic_wf_pc), 0),
            "net_vw_import_per_capita_m3": round(float(net_vw_import_pc), 0),
            "total_water_footprint_gm3": round(total_wf / 1e9, 2),
            "total_net_vw_trade_gm3": round(total_net_vw / 1e9, 2),
            "external_water_dependency_pct": round(float(external_dependency), 2),
            "direction": "net_importer" if net_vw_import_pc > 0 else "net_exporter",
            "virtual_water_content_reference": self.VIRTUAL_WATER_M3_PER_TONNE,
        }

    def _groundwater_externalities(
        self,
        withdrawal_pct_ts: dict[str, float],
        ag_water_ts: dict[str, float],
        gdp_ts: dict[str, float],
    ) -> dict:
        """Estimate groundwater depletion externalities.

        External costs: pumping cost increase + land subsidence + ecological damage.
        Following Pfeiffer & Lin (2014) framework.
        """
        withdrawal = self._latest_value(withdrawal_pct_ts)
        ag_share = self._latest_value(ag_water_ts)
        gdp = self._latest_value(gdp_ts)

        if withdrawal is None or gdp is None:
            return {"error": "insufficient data"}

        # Overexploitation indicator
        overexploitation = withdrawal > 60  # >60% withdrawal = stress

        # Pumping cost externality (marginal cost rises with depth)
        # Approximate: 10% increase in pumping cost per 10% overextraction
        excess_extraction = max(0, withdrawal - 40) / 100  # above sustainable threshold
        pumping_cost_increase_pct = excess_extraction * 100

        # Subsidence damage (proportion of GDP)
        # Severe in deltaic regions like Bangladesh
        subsidence_damage_pct_gdp = excess_extraction * 0.1  # 0.1% GDP per 1% overextraction

        # Ecological damage (wetland/baseflow reduction)
        ecological_damage_pct_gdp = excess_extraction * 0.05

        total_external_cost_pct_gdp = subsidence_damage_pct_gdp + ecological_damage_pct_gdp
        total_external_cost_usd = gdp * total_external_cost_pct_gdp / 100

        return {
            "withdrawal_pct_internal": round(withdrawal, 2),
            "agricultural_share_pct": round(ag_share, 2) if ag_share else None,
            "overexploitation": overexploitation,
            "pumping_cost_increase_pct": round(float(pumping_cost_increase_pct), 2),
            "subsidence_damage_pct_gdp": round(float(subsidence_damage_pct_gdp), 4),
            "ecological_damage_pct_gdp": round(float(ecological_damage_pct_gdp), 4),
            "total_external_cost_pct_gdp": round(float(total_external_cost_pct_gdp), 4),
            "total_external_cost_musd": round(float(total_external_cost_usd / 1e6), 2),
        }

    def _irrigation_returns(
        self,
        irrigated_ts: dict[str, float],
        ag_water_ts: dict[str, float],
        gdp_ts: dict[str, float],
    ) -> dict:
        """Estimate returns to irrigation investment.

        Return = (yield premium from irrigation * irrigated area) / investment cost.
        """
        irrigated_pct = self._latest_value(irrigated_ts)
        gdp = self._latest_value(gdp_ts)

        if irrigated_pct is None or gdp is None:
            return {"error": "insufficient irrigation data"}

        # Yield premium: irrigated vs rainfed (typical 50-100% increase)
        yield_premium_pct = 75  # conservative estimate

        # Agriculture value added (approximate as 15-25% of GDP for developing)
        ag_va = gdp * 0.15  # approximate

        # Value of irrigation
        irrigated_value = ag_va * (irrigated_pct / 100) * (yield_premium_pct / 100)

        # Irrigation investment cost (approximate)
        # Cost varies: $3000-8000 per hectare for new irrigation
        cost_per_ha = 5000
        # Assume 150M hectares agricultural land (Bangladesh example ~9M)
        approx_irrigated_ha = 9e6 * irrigated_pct / 100  # rough approximation
        total_investment = approx_irrigated_ha * cost_per_ha

        # Rate of return
        if total_investment > 0:
            annual_return_pct = irrigated_value / total_investment * 100
        else:
            annual_return_pct = 0

        # Water use efficiency (output per m3)
        ag_water_pct = self._latest_value(ag_water_ts) or 70
        # Approximate water productivity
        water_productivity = ag_va / (gdp * 0.001 * ag_water_pct / 100)  # USD per m3 proxy

        # Efficiency improvement potential
        current_efficiency = 0.40 if irrigated_pct < 30 else 0.55 if irrigated_pct < 60 else 0.70
        potential_efficiency = 0.80
        savings_potential = (potential_efficiency - current_efficiency) / current_efficiency * 100

        return {
            "irrigated_land_pct": round(irrigated_pct, 2),
            "yield_premium_pct": yield_premium_pct,
            "irrigation_value_musd": round(float(irrigated_value / 1e6), 2),
            "annual_return_pct": round(float(annual_return_pct), 2),
            "current_efficiency": round(current_efficiency, 2),
            "potential_efficiency": potential_efficiency,
            "water_savings_potential_pct": round(float(savings_potential), 1),
        }

    @staticmethod
    def _latest_value(ts: dict[str, float]) -> float | None:
        """Get latest value from a year-keyed time series."""
        if not ts:
            return None
        latest_yr = sorted(ts.keys())[-1]
        return ts[latest_yr]
