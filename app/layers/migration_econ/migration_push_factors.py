"""Migration Push Factors module.

Composite measure of economic push conditions that drive emigration:
unemployment, low per-capita income, and poor governance.

High unemployment raises the direct cost of staying. Low GDP per
capita signals limited economic opportunity. Poor government
effectiveness further reduces the expected value of remaining.

Score = weighted composite of normalized negative conditions.

Sources: WDI (SL.UEM.TOTL.ZS, NY.GDP.PCAP.KD, GE.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MigrationPushFactors(LayerBase):
    layer_id = "lME"
    name = "Migration Push Factors"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        unem_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.UEM.TOTL.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        gdp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        gov_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GE.EST'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not unem_rows and not gdp_rows and not gov_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        unem_vals = [float(r["value"]) for r in unem_rows if r["value"] is not None]
        gdp_vals = [float(r["value"]) for r in gdp_rows if r["value"] is not None]
        gov_vals = [float(r["value"]) for r in gov_rows if r["value"] is not None]

        unem = float(np.mean(unem_vals)) if unem_vals else 6.0
        gdp_pc = float(np.mean(gdp_vals)) if gdp_vals else 5000.0
        gov_eff = float(np.mean(gov_vals)) if gov_vals else 0.0

        # Unemployment: 0-30% range -> 0-40 score weight
        unem_score = float(np.clip(unem / 30 * 40, 0, 40))

        # GDP per capita: low income = high push. Normalize: <1000 = max pressure
        # Use log scale: score declines with higher income
        gdp_score = float(np.clip(40 * (1 - np.log1p(gdp_pc) / np.log1p(50000)), 0, 40))

        # Governance: GE.EST -2.5 to +2.5. Poor governance = high push.
        gov_raw = max(0.0, -gov_eff)
        gov_score = float(np.clip(gov_raw * 8, 0, 20))

        score = unem_score + gdp_score + gov_score

        return {
            "score": round(score, 1),
            "country": country,
            "unemployment_pct": round(unem, 2),
            "gdp_per_capita_constant_usd": round(gdp_pc, 0),
            "gov_effectiveness_est": round(gov_eff, 4),
            "components": {
                "unemployment_pressure": round(unem_score, 2),
                "income_pressure": round(gdp_score, 2),
                "governance_pressure": round(gov_score, 2),
            },
            "interpretation": (
                "strong push conditions" if score > 65
                else "moderate push" if score > 40
                else "weak push conditions"
            ),
        }
