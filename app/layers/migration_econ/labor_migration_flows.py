"""Labor Migration Flows module.

Estimates labor migration intensity from youth unemployment and
GDP growth differentials. High youth unemployment combined with low
economic growth creates structural pressure for labor emigration.

Youth unemployment is a leading indicator: young workers facing
no job prospects are the most mobile and most likely to emigrate.
Stagnant or declining GDP per capita amplifies the incentive to leave.

Score = weighted composite of youth unemployment pressure and
negative growth environment.

Sources: WDI (SL.UEM.1524.ZS, NY.GDP.PCAP.KD.ZG)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class LaborMigrationFlows(LayerBase):
    layer_id = "lME"
    name = "Labor Migration Flows"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        youth_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.UEM.1524.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        growth_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD.ZG'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not youth_rows and not growth_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        youth_vals = [float(r["value"]) for r in youth_rows if r["value"] is not None]
        growth_vals = [float(r["value"]) for r in growth_rows if r["value"] is not None]

        youth_unem = float(np.mean(youth_vals)) if youth_vals else 15.0
        gdp_growth = float(np.mean(growth_vals)) if growth_vals else 2.0

        # Youth unemployment: 0-60% range -> 0-60 score weight
        youth_score = float(np.clip(youth_unem / 60 * 60, 0, 60))

        # Growth: negative or very low growth amplifies migration pressure
        # Below 2% = some pressure; below 0% = strong pressure
        growth_penalty = max(0.0, 2.0 - gdp_growth)
        growth_score = float(np.clip(growth_penalty * 10, 0, 40))

        score = youth_score + growth_score

        return {
            "score": round(score, 1),
            "country": country,
            "youth_unemployment_pct": round(youth_unem, 2),
            "gdp_per_capita_growth_pct": round(gdp_growth, 2),
            "components": {
                "youth_unemployment_pressure": round(youth_score, 2),
                "low_growth_pressure": round(growth_score, 2),
            },
            "interpretation": (
                "high labor migration pressure" if score > 65
                else "moderate pressure" if score > 40
                else "low migration pressure"
            ),
        }
