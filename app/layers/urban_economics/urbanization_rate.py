"""Urbanization Rate module.

Measures urban share of population and growth speed.
Rapid urbanization signals infrastructure stress.

Sources: WDI SP.URB.TOTL.IN.ZS (urban pop % of total), SP.URB.GROW (urban pop growth rate).
Score = clip(max(0, urb_growth - 1.5) * 25, 0, 100).
Very rapid urbanization (>3%/yr) = near-maximum infrastructure stress.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class UrbanizationRate(LayerBase):
    layer_id = "lUE"
    name = "Urbanization Rate"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        urb_share_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.URB.TOTL.IN.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        urb_growth_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.URB.GROW'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not urb_share_rows and not urb_growth_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        urb_share = float(urb_share_rows[0]["value"]) if urb_share_rows else None
        urb_growth = float(urb_growth_rows[0]["value"]) if urb_growth_rows else None

        if urb_growth is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no urbanization growth data"}

        score = float(np.clip(max(0.0, urb_growth - 1.5) * 25, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "urban_share_pct": round(urb_share, 2) if urb_share is not None else None,
            "urban_growth_rate_pct": round(urb_growth, 3),
            "stress_threshold_pct_per_yr": 1.5,
            "interpretation": (
                "Very rapid urbanization (>3%/yr) signals acute infrastructure stress"
                if urb_growth > 3.0
                else "Moderate urbanization pace" if urb_growth > 1.5
                else "Slow or stable urbanization"
            ),
            "_sources": ["WDI:SP.URB.TOTL.IN.ZS", "WDI:SP.URB.GROW"],
        }
