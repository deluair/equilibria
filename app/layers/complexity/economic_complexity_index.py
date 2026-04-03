"""Economic Complexity Index module.

Product space complexity proxy using manufacturing and high-tech export shares.
Low complexity economies (commodity-dependent, low-tech) face structural stress.

Score = max(0, 50 - manf_share - hitech_share*2)
High manufacturing + high-tech = low stress. Low both = high stress.

Sources: WDI TX.VAL.MANF.ZS.UN (manufacturing exports % merch), TX.VAL.TECH.MF.ZS (high-tech %)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class EconomicComplexityIndex(LayerBase):
    layer_id = "lCP"
    name = "Economic Complexity Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        manf_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'TX.VAL.MANF.ZS.UN'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        hitech_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'TX.VAL.TECH.MF.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not manf_rows and not hitech_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        manf_share = float(manf_rows[0]["value"]) if manf_rows else 0.0
        hitech_share = float(hitech_rows[0]["value"]) if hitech_rows else 0.0

        manf_date = manf_rows[0]["date"] if manf_rows else None
        hitech_date = hitech_rows[0]["date"] if hitech_rows else None

        score = float(max(0.0, 50.0 - manf_share - hitech_share * 2.0))
        score = min(100.0, score)

        return {
            "score": round(score, 1),
            "country": country,
            "manufacturing_export_share_pct": round(manf_share, 2),
            "hightech_export_share_pct": round(hitech_share, 2),
            "manf_date": manf_date,
            "hitech_date": hitech_date,
            "interpretation": (
                "Low score = high complexity (diverse, high-tech exports). "
                "High score = low complexity (commodity/low-tech concentration)."
            ),
            "_citation": "World Bank WDI: TX.VAL.MANF.ZS.UN, TX.VAL.TECH.MF.ZS",
        }
