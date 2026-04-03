"""Green growth: decoupling indicators, green GDP, inclusive wealth, genuine savings.

Measures the extent to which economic growth has decoupled from environmental
degradation. Computes decoupling elasticities (CO2/GDP), green GDP adjustments,
inclusive wealth index following UNU-IHDP/UNEP methodology, and genuine savings
(adjusted net savings) following World Bank methodology.

Methodology:
    Decoupling elasticity:
        e = (dE/E) / (dY/Y)
        e < 1: relative decoupling
        e < 0: absolute decoupling

    Green GDP = GDP - depreciation of natural capital - environmental damage costs.

    Inclusive Wealth Index (IWI): produced capital + human capital + natural capital.

    Genuine Savings (Adjusted Net Savings):
        GS = GNS - Dh + CSE - RD - CD
        where GNS = gross national savings, Dh = depreciation, CSE = education
        expenditure, RD = resource depletion, CD = CO2 damage.

References:
    OECD (2002). "Indicators to Measure Decoupling of Environmental Pressure
        from Economic Growth." OECD Environment Directorate.
    UNU-IHDP/UNEP (2014). "Inclusive Wealth Report 2014." Cambridge University Press.
    World Bank (2021). "The Changing Wealth of Nations 2021."
    Hamilton, K. & Clemens, M. (1999). "Genuine savings rates in developing
        countries." World Bank Economic Review, 13(2), 333-356.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class GreenGrowth(LayerBase):
    layer_id = "l9"
    name = "Green Growth"

    # Series IDs for green growth indicators
    SERIES_MAP = {
        "co2_kt": "EN.ATM.CO2E.KT",
        "gdp": "NY.GDP.MKTP.KD",
        "gdp_pc": "NY.GDP.PCAP.KD",
        "gns_pct": "NY.GNS.ICTR.ZS",  # Gross national savings (% of GNI)
        "depreciation_pct": "NY.ADJ.DKAP.GN.ZS",  # Depreciation (% of GNI)
        "education_exp": "NY.ADJ.AEDU.GN.ZS",  # Education expenditure (% of GNI)
        "resource_depletion": "NY.ADJ.DRES.GN.ZS",  # Resource depletion (% of GNI)
        "co2_damage": "NY.ADJ.DCO2.GN.ZS",  # CO2 damage (% of GNI)
        "adj_net_savings": "NY.ADJ.SVNX.GN.ZS",  # Adjusted net savings (% of GNI)
        "forest_rent": "NY.GDP.FRST.RT.ZS",  # Forest rents (% of GDP)
        "mineral_rent": "NY.GDP.MINR.RT.ZS",  # Mineral rents (% of GDP)
        "energy_rent": "NY.GDP.TOTL.RT.ZS",  # Total natural resources rents (% of GDP)
    }

    async def compute(self, db, **kwargs) -> dict:
        """Compute green growth indicators.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
        """
        country = kwargs.get("country_iso3", "BGD")

        series_ids = list(self.SERIES_MAP.values())
        placeholders = ",".join(["?" for _ in series_ids])

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
            (country, *series_ids),
        )

        if not rows or len(rows) < 10:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient green growth data"}

        # Parse into time series
        data: dict[str, dict[str, float]] = {}
        for r in rows:
            sid = r["series_id"]
            yr = r["date"][:4] if isinstance(r["date"], str) else str(r["date"])
            data.setdefault(sid, {})[yr] = float(r["value"])

        co2_ts = data.get(self.SERIES_MAP["co2_kt"], {})
        gdp_ts = data.get(self.SERIES_MAP["gdp"], {})

        # 1. Decoupling indicators
        decoupling = self._compute_decoupling(co2_ts, gdp_ts)

        # 2. Green GDP adjustment
        green_gdp = self._compute_green_gdp(
            gdp_ts=gdp_ts,
            co2_damage_ts=data.get(self.SERIES_MAP["co2_damage"], {}),
            resource_depletion_ts=data.get(self.SERIES_MAP["resource_depletion"], {}),
            depreciation_ts=data.get(self.SERIES_MAP["depreciation_pct"], {}),
        )

        # 3. Genuine savings
        genuine_savings = self._compute_genuine_savings(
            gns_ts=data.get(self.SERIES_MAP["gns_pct"], {}),
            depreciation_ts=data.get(self.SERIES_MAP["depreciation_pct"], {}),
            education_ts=data.get(self.SERIES_MAP["education_exp"], {}),
            resource_ts=data.get(self.SERIES_MAP["resource_depletion"], {}),
            co2_ts=data.get(self.SERIES_MAP["co2_damage"], {}),
            adj_ts=data.get(self.SERIES_MAP["adj_net_savings"], {}),
        )

        # 4. Inclusive Wealth approximation
        iwi = self._approximate_iwi(
            gdp_ts=gdp_ts,
            forest_rent_ts=data.get(self.SERIES_MAP["forest_rent"], {}),
            mineral_rent_ts=data.get(self.SERIES_MAP["mineral_rent"], {}),
            energy_rent_ts=data.get(self.SERIES_MAP["energy_rent"], {}),
        )

        # Score: composite of decoupling + genuine savings + green GDP gap
        decoupling_score = 0.0
        if decoupling.get("latest_elasticity") is not None:
            e = decoupling["latest_elasticity"]
            if e < 0:
                decoupling_score = 15  # absolute decoupling (good)
            elif e < 0.5:
                decoupling_score = 30  # strong relative decoupling
            elif e < 1.0:
                decoupling_score = 50  # weak relative decoupling
            else:
                decoupling_score = 70  # no decoupling

        gs_score = 0.0
        if genuine_savings.get("latest_adj_net_savings_pct") is not None:
            gs = genuine_savings["latest_adj_net_savings_pct"]
            if gs > 10:
                gs_score = 15
            elif gs > 5:
                gs_score = 25
            elif gs > 0:
                gs_score = 40
            else:
                gs_score = 60  # negative genuine savings = unsustainable

        score = float(np.clip((decoupling_score + gs_score) / 2, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "decoupling": decoupling,
            "green_gdp": green_gdp,
            "genuine_savings": genuine_savings,
            "inclusive_wealth": iwi,
        }

    @staticmethod
    def _compute_decoupling(
        co2_ts: dict[str, float], gdp_ts: dict[str, float]
    ) -> dict:
        """Compute decoupling elasticity (CO2 growth / GDP growth)."""
        common = sorted(set(co2_ts.keys()) & set(gdp_ts.keys()))
        if len(common) < 5:
            return {"error": "insufficient matched CO2-GDP data"}

        co2 = np.array([co2_ts[y] for y in common])
        gdp = np.array([gdp_ts[y] for y in common])
        years = np.array([int(y) for y in common])

        # Carbon intensity
        intensity = co2 / gdp * 1e6  # tCO2 per million USD

        # Growth rates
        co2_growth = np.diff(co2) / co2[:-1]
        gdp_growth = np.diff(gdp) / gdp[:-1]

        # Period elasticities
        valid = gdp_growth != 0
        elasticities = np.where(valid, co2_growth / gdp_growth, np.nan)

        # 5-year rolling elasticity
        window = min(5, len(elasticities))
        if window > 0:
            latest_e = float(np.nanmean(elasticities[-window:]))
        else:
            latest_e = None

        # Trend in carbon intensity (linear regression)
        if len(intensity) >= 3:
            ci_trend = np.polyfit(years - years[0], intensity, 1)
            ci_slope = float(ci_trend[0])
        else:
            ci_slope = None

        # Classify decoupling
        if latest_e is not None:
            if latest_e < 0:
                status = "absolute_decoupling"
            elif latest_e < 0.5:
                status = "strong_relative_decoupling"
            elif latest_e < 1.0:
                status = "weak_relative_decoupling"
            else:
                status = "no_decoupling"
        else:
            status = "unknown"

        return {
            "latest_elasticity": round(latest_e, 4) if latest_e is not None else None,
            "status": status,
            "carbon_intensity_latest": round(float(intensity[-1]), 2),
            "carbon_intensity_trend_slope": round(ci_slope, 4) if ci_slope is not None else None,
            "n_years": len(common),
        }

    @staticmethod
    def _compute_green_gdp(
        gdp_ts: dict[str, float],
        co2_damage_ts: dict[str, float],
        resource_depletion_ts: dict[str, float],
        depreciation_ts: dict[str, float],
    ) -> dict:
        """Compute Green GDP = GDP - environmental damage - resource depletion."""
        common = sorted(
            set(gdp_ts.keys()) & set(co2_damage_ts.keys()) &
            set(resource_depletion_ts.keys())
        )
        if not common:
            return {"error": "insufficient data for green GDP"}

        latest_yr = common[-1]
        gdp = gdp_ts[latest_yr]
        co2_dmg_pct = co2_damage_ts.get(latest_yr, 0)
        res_dep_pct = resource_depletion_ts.get(latest_yr, 0)
        depr_pct = depreciation_ts.get(latest_yr, 0)

        env_cost_pct = co2_dmg_pct + res_dep_pct
        green_gdp_pct = 100.0 - env_cost_pct - depr_pct
        green_gdp_usd = gdp * green_gdp_pct / 100.0

        return {
            "year": latest_yr,
            "gdp_usd": round(gdp, 0),
            "green_gdp_usd": round(green_gdp_usd, 0),
            "green_gdp_pct_of_gdp": round(green_gdp_pct, 2),
            "environmental_cost_pct_gni": round(env_cost_pct, 2),
            "co2_damage_pct_gni": round(co2_dmg_pct, 2),
            "resource_depletion_pct_gni": round(res_dep_pct, 2),
        }

    @staticmethod
    def _compute_genuine_savings(
        gns_ts: dict[str, float],
        depreciation_ts: dict[str, float],
        education_ts: dict[str, float],
        resource_ts: dict[str, float],
        co2_ts: dict[str, float],
        adj_ts: dict[str, float],
    ) -> dict:
        """Compute genuine savings (adjusted net savings) decomposition."""
        # Use WDI pre-computed if available
        if adj_ts:
            years = sorted(adj_ts.keys())
            latest = years[-1]
            vals = np.array([adj_ts[y] for y in years])
            trend = np.polyfit(np.arange(len(vals)), vals, 1) if len(vals) >= 3 else [0, 0]

            return {
                "latest_year": latest,
                "latest_adj_net_savings_pct": round(adj_ts[latest], 2),
                "trend_slope": round(float(trend[0]), 4),
                "sustainable": adj_ts[latest] > 0,
                "n_years": len(years),
            }

        # Manual decomposition
        common = sorted(
            set(gns_ts.keys()) & set(depreciation_ts.keys())
        )
        if not common:
            return {"error": "insufficient savings data"}

        latest = common[-1]
        gns = gns_ts.get(latest, 0)
        depr = depreciation_ts.get(latest, 0)
        edu = education_ts.get(latest, 0)
        res = resource_ts.get(latest, 0)
        co2 = co2_ts.get(latest, 0)

        genuine = gns - depr + edu - res - co2

        return {
            "latest_year": latest,
            "gross_national_savings_pct": round(gns, 2),
            "depreciation_pct": round(depr, 2),
            "education_expenditure_pct": round(edu, 2),
            "resource_depletion_pct": round(res, 2),
            "co2_damage_pct": round(co2, 2),
            "latest_adj_net_savings_pct": round(genuine, 2),
            "sustainable": genuine > 0,
        }

    @staticmethod
    def _approximate_iwi(
        gdp_ts: dict[str, float],
        forest_rent_ts: dict[str, float],
        mineral_rent_ts: dict[str, float],
        energy_rent_ts: dict[str, float],
    ) -> dict:
        """Approximate Inclusive Wealth Index components.

        Uses natural resource rents as proxy for natural capital value,
        GDP level as proxy for produced capital, and residual for human capital.
        """
        common = sorted(set(gdp_ts.keys()) & set(energy_rent_ts.keys()))
        if not common:
            return {"error": "insufficient data for IWI"}

        latest = common[-1]
        gdp = gdp_ts[latest]

        # Natural capital proxy: total resource rents * GDP / discount_rate
        total_rent_pct = energy_rent_ts.get(latest, 0)
        natural_capital = gdp * total_rent_pct / 100.0 / 0.05  # 5% discount rate

        # Produced capital proxy: ~3x GDP (typical capital-output ratio)
        produced_capital = gdp * 3.0

        # Human capital proxy: residual (IWI - produced - natural)
        # Approximate using education-augmented labor value
        human_capital = gdp * 2.5  # typical for developing countries

        total_wealth = produced_capital + human_capital + natural_capital

        return {
            "year": latest,
            "produced_capital_usd": round(produced_capital, 0),
            "human_capital_usd": round(human_capital, 0),
            "natural_capital_usd": round(natural_capital, 0),
            "total_inclusive_wealth_usd": round(total_wealth, 0),
            "natural_capital_share_pct": round(natural_capital / total_wealth * 100, 2) if total_wealth > 0 else 0,
            "resource_rents_pct_gdp": round(total_rent_pct, 2),
        }
