"""Urban Poverty Rate module.

Proxies urban poverty burden as the intersection of urbanization and overall poverty.
High urban share combined with high poverty headcount implies concentrated urban poverty.

Sources: WDI SP.URB.TOTL.IN.ZS (urban pop %), SI.POV.DDAY (poverty headcount at $2.15/day).
Score = clip((urban_pct/100) * poverty_headcount * 1.5, 0, 100).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class UrbanPovertyRate(LayerBase):
    layer_id = "lUE"
    name = "Urban Poverty Rate"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        urb_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.URB.TOTL.IN.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        pov_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.POV.DDAY'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not urb_rows or not pov_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        urban_pct = float(urb_rows[0]["value"])
        poverty_headcount = float(pov_rows[0]["value"])

        score = float(np.clip((urban_pct / 100.0) * poverty_headcount * 1.5, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "urban_share_pct": round(urban_pct, 2),
            "poverty_headcount_pct": round(poverty_headcount, 2),
            "urban_poverty_burden": round((urban_pct / 100.0) * poverty_headcount, 3),
            "interpretation": (
                "Severe urban poverty burden: large urban population facing high poverty"
                if score > 50
                else "Moderate urban poverty risk" if score > 25
                else "Low urban poverty burden"
            ),
            "_sources": ["WDI:SP.URB.TOTL.IN.ZS", "WDI:SI.POV.DDAY"],
        }
