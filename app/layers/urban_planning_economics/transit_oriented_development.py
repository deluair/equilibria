"""Transit Oriented Development module.

Measures alignment between road infrastructure and urban population concentration.
Low paved road share in a highly urbanized country signals poor transit-oriented planning.

Sources: WDI IS.ROD.PAVE.ZS (paved roads % of total), SP.URB.TOTL.IN.ZS (urban pop %).
Score = infrastructure gap = clip((urb_share - paved_roads_share) * 1.5, 0, 100).
High urbanization with low road pavement = transit infrastructure deficit.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TransitOrientedDevelopment(LayerBase):
    layer_id = "lUP"
    name = "Transit Oriented Development"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        road_rows = await db.fetch_all(
            """
            SELECT dp.value FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'IS.ROD.PAVE.ZS'
            ORDER BY dp.date DESC LIMIT 15
            """,
            (country,),
        )

        urb_rows = await db.fetch_all(
            """
            SELECT dp.value FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'SP.URB.TOTL.IN.ZS'
            ORDER BY dp.date DESC LIMIT 15
            """,
            (country,),
        )

        if not road_rows or not urb_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data for transit oriented development"}

        paved_pct = float(road_rows[0]["value"])
        urb_share = float(urb_rows[0]["value"])

        # Gap between urbanization and road infrastructure quality
        gap = urb_share - paved_pct
        score = float(np.clip(max(0.0, gap) * 1.5, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "paved_roads_pct": round(paved_pct, 2),
            "urban_share_pct": round(urb_share, 2),
            "infrastructure_gap_ppt": round(gap, 2),
            "interpretation": (
                "Severe transit infrastructure deficit relative to urbanization"
                if score > 60
                else "Moderate transit-urbanization mismatch"
                if score > 30
                else "Road infrastructure broadly aligned with urbanization"
            ),
            "_sources": ["WDI:IS.ROD.PAVE.ZS", "WDI:SP.URB.TOTL.IN.ZS"],
        }
