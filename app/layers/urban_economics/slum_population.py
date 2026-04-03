"""Slum Population module.

Measures informal settlement share of urban population.
High slum population indicates urban informality crisis.

Sources: WDI EN.POP.SLUM.UR.ZS (population living in slums, % of urban population).
Score = clip(slum_pct * 1.2, 0, 100).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SlumPopulation(LayerBase):
    layer_id = "lUE"
    name = "Slum Population"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'EN.POP.SLUM.UR.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no slum population data"}

        slum_pct = float(rows[0]["value"])
        date = rows[0]["date"]

        # Trend: compute change if multiple observations available
        trend = None
        if len(rows) >= 2:
            oldest = float(rows[-1]["value"])
            trend = round(slum_pct - oldest, 2)

        score = float(np.clip(slum_pct * 1.2, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "slum_population_pct": round(slum_pct, 2),
            "reference_date": date,
            "trend_ppt_change": trend,
            "interpretation": (
                "Acute urban informality crisis: majority of urban dwellers in slums"
                if slum_pct > 60
                else "High informal settlement prevalence" if slum_pct > 30
                else "Moderate slum presence" if slum_pct > 10
                else "Low informal settlement share"
            ),
            "_sources": ["WDI:EN.POP.SLUM.UR.ZS"],
        }
