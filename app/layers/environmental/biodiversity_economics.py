"""Biodiversity economics: TEEB valuation, species-area, offset markets, Dasgupta framework.

Estimates the economic value of ecosystem services using TEEB methodology,
models species-area relationships for habitat loss impacts, evaluates
biodiversity offset markets, and applies the Dasgupta (2021) framework
for measuring natural capital in economic accounts.

Methodology:
    Species-Area Relationship (SAR):
        S = c * A^z
        where S = species count, A = area, z ~ 0.15-0.35, c = taxon constant.
        Species loss from habitat destruction: dS/S = z * dA/A.

    TEEB ecosystem service valuation: benefit transfer of per-hectare
    values for provisioning, regulating, cultural, and supporting services.

    Dasgupta (2021) framework: natural capital as a productive asset
    in the inclusive wealth function, with depreciation from extraction
    and appreciation from regeneration.

References:
    Dasgupta, P. (2021). "The Economics of Biodiversity: The Dasgupta Review."
        HM Treasury, UK Government.
    TEEB (2010). "The Economics of Ecosystems and Biodiversity: Mainstreaming."
    Arrhenius, O. (1921). "Species and area." Journal of Ecology, 9(1), 95-99.
    Costanza, R. et al. (2014). "Changes in the global value of ecosystem services."
        Global Environmental Change, 26, 152-158.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class BiodiversityEconomics(LayerBase):
    layer_id = "l9"
    name = "Biodiversity Economics"

    # TEEB ecosystem service values (2020 USD per hectare per year)
    # Based on Costanza et al. (2014) and de Groot et al. (2012)
    ECOSYSTEM_VALUES_USD_HA_YR = {
        "tropical_forest": {
            "provisioning": 1200,
            "regulating": 5400,
            "cultural": 800,
            "supporting": 2100,
        },
        "mangrove": {
            "provisioning": 2400,
            "regulating": 8300,
            "cultural": 400,
            "supporting": 1800,
        },
        "wetland": {
            "provisioning": 1800,
            "regulating": 15000,
            "cultural": 3200,
            "supporting": 5400,
        },
        "coral_reef": {
            "provisioning": 4200,
            "regulating": 6300,
            "cultural": 11500,
            "supporting": 7800,
        },
        "grassland": {
            "provisioning": 600,
            "regulating": 1200,
            "cultural": 300,
            "supporting": 400,
        },
        "cropland": {
            "provisioning": 2800,
            "regulating": 200,
            "cultural": 100,
            "supporting": 300,
        },
    }

    # SAR exponent z by region type
    SAR_Z_VALUES = {
        "island": 0.35,
        "mainland_fragment": 0.25,
        "continental": 0.15,
    }

    async def compute(self, db, **kwargs) -> dict:
        """Compute biodiversity economics analysis.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            ecosystem_type : str - primary ecosystem (default "tropical_forest")
            region_type : str - SAR region type (default "mainland_fragment")
        """
        country = kwargs.get("country_iso3", "BGD")
        ecosystem_type = kwargs.get("ecosystem_type", "tropical_forest")
        region_type = kwargs.get("region_type", "mainland_fragment")

        # Fetch land use and environmental data
        land_series = [
            "AG.LND.FRST.ZS",   # Forest area (% of land area)
            "AG.LND.FRST.K2",   # Forest area (sq km)
            "AG.LND.TOTL.K2",   # Land area (sq km)
            "AG.LND.ARBL.ZS",   # Arable land (% of land area)
            "ER.PTD.TOTL.ZS",   # Terrestrial protected areas (%)
            "NY.GDP.MKTP.KD",   # GDP
            "NY.GDP.PCAP.KD",   # GDP per capita
            "SP.POP.TOTL",      # Population
        ]
        placeholders = ",".join(["?" for _ in land_series])

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
            (country, *land_series),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient land/environmental data"}

        data: dict[str, dict[str, float]] = {}
        for r in rows:
            sid = r["series_id"]
            yr = r["date"][:4] if isinstance(r["date"], str) else str(r["date"])
            data.setdefault(sid, {})[yr] = float(r["value"])

        forest_pct_ts = data.get("AG.LND.FRST.ZS", {})
        forest_km2_ts = data.get("AG.LND.FRST.K2", {})
        land_km2_ts = data.get("AG.LND.TOTL.K2", {})
        protected_ts = data.get("ER.PTD.TOTL.ZS", {})
        gdp_ts = data.get("NY.GDP.MKTP.KD", {})

        # 1. TEEB ecosystem service valuation
        teeb_valuation = self._teeb_valuation(
            ecosystem_type=ecosystem_type,
            forest_km2_ts=forest_km2_ts,
            land_km2_ts=land_km2_ts,
            gdp_ts=gdp_ts,
        )

        # 2. Species-area relationship (habitat loss impact)
        sar_analysis = self._species_area(
            forest_pct_ts=forest_pct_ts,
            z=self.SAR_Z_VALUES.get(region_type, 0.25),
        )

        # 3. Biodiversity offset market estimation
        offset_market = self._estimate_offset_market(
            forest_km2_ts=forest_km2_ts,
            gdp_ts=gdp_ts,
            ecosystem_type=ecosystem_type,
        )

        # 4. Dasgupta framework: natural capital accounting
        dasgupta = self._dasgupta_natural_capital(
            forest_km2_ts=forest_km2_ts,
            land_km2_ts=land_km2_ts,
            gdp_ts=gdp_ts,
            protected_ts=protected_ts,
            ecosystem_type=ecosystem_type,
        )

        # Score: high habitat loss + low protection + declining natural capital = stress
        habitat_loss_score = min(40, sar_analysis.get("cumulative_species_loss_pct", 0) * 2)
        protection_gap = max(0, 30 - (self._latest_value(protected_ts) or 0))  # Kunming 30% target
        protection_score = min(30, protection_gap)
        nk_decline = 1 if dasgupta.get("natural_capital_trend") == "declining" else 0
        nk_score = nk_decline * 30

        score = float(np.clip(habitat_loss_score + protection_score + nk_score, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "ecosystem_type": ecosystem_type,
            "teeb_valuation": teeb_valuation,
            "species_area": sar_analysis,
            "offset_market": offset_market,
            "dasgupta_framework": dasgupta,
        }

    def _teeb_valuation(
        self,
        ecosystem_type: str,
        forest_km2_ts: dict[str, float],
        land_km2_ts: dict[str, float],
        gdp_ts: dict[str, float],
    ) -> dict:
        """TEEB-based ecosystem service valuation via benefit transfer."""
        values = self.ECOSYSTEM_VALUES_USD_HA_YR.get(ecosystem_type, {})
        if not values:
            return {"error": f"no TEEB values for {ecosystem_type}"}

        total_per_ha = sum(values.values())
        latest_forest = self._latest_value(forest_km2_ts)
        latest_gdp = self._latest_value(gdp_ts)

        if latest_forest is None:
            return {"error": "no forest area data"}

        area_ha = latest_forest * 100  # km2 to hectares
        total_value = total_per_ha * area_ha

        return {
            "ecosystem_type": ecosystem_type,
            "area_km2": round(latest_forest, 2),
            "area_ha": round(area_ha, 0),
            "value_per_ha_yr_usd": total_per_ha,
            "value_breakdown_usd_ha": values,
            "total_annual_value_musd": round(total_value / 1e6, 2),
            "pct_of_gdp": round(total_value / latest_gdp * 100, 2) if latest_gdp else None,
        }

    @staticmethod
    def _species_area(
        forest_pct_ts: dict[str, float], z: float
    ) -> dict:
        """Species-area relationship analysis of habitat loss.

        S = c * A^z => dS/S = z * dA/A
        """
        if not forest_pct_ts or len(forest_pct_ts) < 2:
            return {"error": "insufficient forest time series"}

        years = sorted(forest_pct_ts.keys())
        vals = np.array([forest_pct_ts[y] for y in years])

        # Forest loss relative to earliest observation
        baseline = vals[0]
        if baseline <= 0:
            return {"error": "invalid baseline forest area"}

        area_ratio = vals / baseline
        # Species retained (SAR): S_retained/S_0 = (A/A_0)^z
        species_retained = area_ratio ** z
        species_loss_pct = (1 - species_retained) * 100

        # Rate of forest loss
        if len(vals) >= 3:
            annual_loss_rate = np.polyfit(np.arange(len(vals)), vals, 1)[0]
        else:
            annual_loss_rate = (vals[-1] - vals[0]) / max(len(vals) - 1, 1)

        # Project species loss under continued deforestation
        if annual_loss_rate < 0:
            years_to_50pct = (baseline * 0.5 - vals[-1]) / annual_loss_rate if annual_loss_rate != 0 else None
        else:
            years_to_50pct = None

        return {
            "sar_z_exponent": z,
            "baseline_forest_pct": round(float(baseline), 2),
            "latest_forest_pct": round(float(vals[-1]), 2),
            "area_loss_pct": round(float((1 - area_ratio[-1]) * 100), 2),
            "cumulative_species_loss_pct": round(float(species_loss_pct[-1]), 2),
            "annual_forest_change_pp": round(float(annual_loss_rate), 3),
            "projected_years_to_50pct_loss": round(float(years_to_50pct), 0) if years_to_50pct is not None else None,
        }

    def _estimate_offset_market(
        self,
        forest_km2_ts: dict[str, float],
        gdp_ts: dict[str, float],
        ecosystem_type: str,
    ) -> dict:
        """Estimate biodiversity offset market potential.

        Offset price = ecosystem service value * multiplier (for additionality
        and permanence requirements, typically 2-3x).
        """
        values = self.ECOSYSTEM_VALUES_USD_HA_YR.get(ecosystem_type, {})
        total_per_ha = sum(values.values()) if values else 5000

        # Offset price includes additionality premium
        offset_price_per_ha = total_per_ha * 2.5  # multiplier for offset quality
        # 20-year credit period
        offset_credit_20yr = offset_price_per_ha * 20

        latest_forest = self._latest_value(forest_km2_ts)
        latest_gdp = self._latest_value(gdp_ts)

        # Potential market size: assume 5% of remaining forest eligible
        if latest_forest:
            eligible_ha = latest_forest * 100 * 0.05
            market_size = eligible_ha * offset_price_per_ha
        else:
            eligible_ha = None
            market_size = None

        return {
            "offset_price_per_ha_yr_usd": round(offset_price_per_ha, 0),
            "offset_credit_20yr_per_ha_usd": round(offset_credit_20yr, 0),
            "eligible_area_ha": round(eligible_ha, 0) if eligible_ha else None,
            "potential_annual_market_musd": round(market_size / 1e6, 2) if market_size else None,
            "pct_of_gdp": round(market_size / latest_gdp * 100, 4) if market_size and latest_gdp else None,
        }

    def _dasgupta_natural_capital(
        self,
        forest_km2_ts: dict[str, float],
        land_km2_ts: dict[str, float],
        gdp_ts: dict[str, float],
        protected_ts: dict[str, float],
        ecosystem_type: str,
    ) -> dict:
        """Dasgupta framework: natural capital as productive asset.

        NK_t = NK_{t-1} + R_t - E_t
        where R = regeneration, E = extraction/degradation.
        Value of NK = discounted flow of ecosystem services.
        """
        latest_forest = self._latest_value(forest_km2_ts)
        latest_land = self._latest_value(land_km2_ts)
        latest_gdp = self._latest_value(gdp_ts)
        latest_protected = self._latest_value(protected_ts)

        values = self.ECOSYSTEM_VALUES_USD_HA_YR.get(ecosystem_type, {})
        value_per_ha = sum(values.values()) if values else 5000

        if latest_forest is None or latest_gdp is None:
            return {"error": "insufficient data for Dasgupta accounting"}

        # Natural capital stock (NPV of ecosystem service flows)
        discount_rate = 0.03
        horizon = 50
        annuity_factor = (1 - (1 + discount_rate) ** (-horizon)) / discount_rate
        forest_ha = latest_forest * 100
        natural_capital_stock = forest_ha * value_per_ha * annuity_factor

        # NK as share of total wealth
        produced_capital = latest_gdp * 3.0  # typical K/Y ratio
        total_wealth = produced_capital + natural_capital_stock
        nk_share = natural_capital_stock / total_wealth * 100 if total_wealth > 0 else 0

        # Trend in natural capital
        if len(forest_km2_ts) >= 3:
            years = sorted(forest_km2_ts.keys())
            vals = np.array([forest_km2_ts[y] for y in years])
            trend = np.polyfit(np.arange(len(vals)), vals, 1)[0]
            nk_trend = "declining" if trend < -0.1 else "stable" if abs(trend) < 0.1 else "increasing"
        else:
            nk_trend = "unknown"

        # Protection gap (Kunming-Montreal target: 30% by 2030)
        protection_gap = max(0, 30 - (latest_protected or 0))

        return {
            "natural_capital_stock_musd": round(natural_capital_stock / 1e6, 2),
            "nk_share_of_wealth_pct": round(nk_share, 2),
            "natural_capital_trend": nk_trend,
            "protected_area_pct": round(latest_protected, 2) if latest_protected else None,
            "kunming_target_gap_pp": round(protection_gap, 2),
            "forest_area_km2": round(latest_forest, 2) if latest_forest else None,
        }

    @staticmethod
    def _latest_value(ts: dict[str, float]) -> float | None:
        """Get latest value from a year-keyed time series."""
        if not ts:
            return None
        latest_yr = sorted(ts.keys())[-1]
        return ts[latest_yr]
