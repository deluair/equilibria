"""Rental Market module.

Proxies rental affordability stress via urban population share and income
inequality. High urbanization combined with high Gini coefficient signals
rental market stress for lower-income urban residents.

Queries:
- SP.URB.TOTL.IN.ZS: urban population (% of total)
- SI.POV.GINI: Gini index

Score = clip((urban_pct / 100) * (gini / 100) * 100 * 1.5, 0, 100)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RentalMarket(LayerBase):
    layer_id = "lRE"
    name = "Rental Market"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        urban_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.URB.TOTL.IN.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        gini_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.POV.GINI'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not urban_rows or len(urban_rows) < 1:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient urban population data for rental market analysis",
            }

        if not gini_rows or len(gini_rows) < 1:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient Gini data for rental market analysis",
            }

        urban_vals = np.array([float(r["value"]) for r in urban_rows])
        gini_vals = np.array([float(r["value"]) for r in gini_rows])

        urban_pct = float(urban_vals[-1])
        gini = float(gini_vals[-1])

        raw_score = (urban_pct / 100.0) * (gini / 100.0) * 100.0 * 1.5
        score = float(np.clip(raw_score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "urban_population_pct": round(urban_pct, 2),
            "gini_index": round(gini, 2),
            "rental_stress_composite": round((urban_pct / 100.0) * (gini / 100.0) * 100.0, 3),
            "n_urban_obs": len(urban_rows),
            "n_gini_obs": len(gini_rows),
            "methodology": "score = clip((urban_pct/100) * (gini/100) * 100 * 1.5, 0, 100)",
        }
