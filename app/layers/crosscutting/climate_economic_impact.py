"""Climate-Economic Impact module.

CO2 emissions growth vs GDP growth decoupling (Tapio 2005).

Queries CO2 emissions per capita (EN.ATM.CO2E.PC) and GDP growth
(NY.GDP.MKTP.KD.ZG). Decoupling occurs when GDP grows while
emissions fall or grow slower. No decoupling (emissions rise with
GDP, high positive correlation) signals a carbon-intensive growth
path and climate-economic stress.

Score rises when emissions and GDP growth are strongly positively
correlated (coupled) or when emissions are accelerating relative
to GDP.
"""

from __future__ import annotations

import numpy as np
from scipy.stats import pearsonr

from app.layers.base import LayerBase


class ClimateEconomicImpact(LayerBase):
    layer_id = "lCX"
    name = "Climate-Economic Impact"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows_co2 = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'EN.ATM.CO2E.PC'
            ORDER BY dp.date
            """,
            (country,),
        )

        rows_gdp = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows_co2 or not rows_gdp:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data for CO2 or GDP growth",
            }

        co2_map = {r["date"]: float(r["value"]) for r in rows_co2 if r["value"] is not None}
        gdp_map = {r["date"]: float(r["value"]) for r in rows_gdp if r["value"] is not None}

        common_dates = sorted(set(co2_map) & set(gdp_map))
        if len(common_dates) < 8:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"only {len(common_dates)} overlapping observations (need 8+)",
            }

        co2_vals = np.array([co2_map[d] for d in common_dates])
        gdp_vals = np.array([gdp_map[d] for d in common_dates])

        # Year-over-year changes for decoupling analysis
        co2_changes = np.diff(co2_vals)
        gdp_changes = gdp_vals[1:]  # GDP already in growth rate terms

        corr = 0.0
        p_value = 1.0
        if len(co2_changes) >= 5:
            corr, p_value = pearsonr(co2_changes, gdp_changes)

        # Tapio decoupling index: ratio of emissions elasticity to GDP
        # If CO2 grows faster than GDP -> strong coupling stress
        co2_trend = float(np.mean(co2_changes)) if len(co2_changes) > 0 else 0.0
        gdp_mean = float(np.mean(gdp_vals))

        # Coupling score: strong positive corr -> high stress (no decoupling)
        coupling_score = float(np.clip((corr + 1.0) / 2.0 * 60.0, 0.0, 60.0))

        # Emissions level penalty: rising per-capita CO2
        emissions_trend_penalty = 0.0
        if co2_trend > 0.1:  # rising per-capita emissions
            emissions_trend_penalty = min(40.0, co2_trend * 20.0)

        score = min(100.0, coupling_score + emissions_trend_penalty)

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(common_dates),
            "period": f"{common_dates[0]} to {common_dates[-1]}",
            "co2_pc_mean": round(float(np.mean(co2_vals)), 3),
            "co2_pc_trend": round(float(co2_trend), 4),
            "gdp_growth_mean": round(float(gdp_mean), 2),
            "emission_gdp_correlation": round(float(corr), 4),
            "p_value": round(float(p_value), 4),
            "decoupling_status": (
                "absolute decoupling" if corr < -0.2 and co2_trend < 0
                else "relative decoupling" if corr < 0.2
                else "coupled growth"
            ),
            "reference": "Tapio 2005, Energy Policy 33(6); Stern 2017",
        }
