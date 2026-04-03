"""Zoning Flexibility Index module.

Measures regulatory environment quality as a proxy for zoning system flexibility.
Rigid, corrupt, or low-quality regulatory environments suppress adaptive zoning.

Sources: World Governance Indicators RQ.EST (regulatory quality), IC.BUS.EASE.XQ (ease of doing business).
Score = clip((100 - normalized_regulatory_quality) * 0.85, 0, 100).
Lower regulatory quality = higher zoning rigidity stress.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ZoningFlexibilityIndex(LayerBase):
    layer_id = "lUP"
    name = "Zoning Flexibility Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rq_rows = await db.fetch_all(
            """
            SELECT dp.value FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'RQ.EST'
            ORDER BY dp.date DESC LIMIT 15
            """,
            (country,),
        )

        biz_rows = await db.fetch_all(
            """
            SELECT dp.value FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'IC.BUS.EASE.XQ'
            ORDER BY dp.date DESC LIMIT 15
            """,
            (country,),
        )

        if not rq_rows and not biz_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no regulatory quality data for zoning flexibility"}

        scores = []
        rq_val = None
        biz_val = None

        if rq_rows:
            # RQ.EST is WGI: roughly -2.5 to +2.5. Normalize to 0-100.
            rq_raw = float(rq_rows[0]["value"])
            rq_val = round(rq_raw, 3)
            rq_norm = np.clip((rq_raw + 2.5) / 5.0, 0, 1) * 100
            # Higher regulatory quality = lower zoning rigidity = lower stress
            scores.append(100 - rq_norm)

        if biz_rows:
            # IC.BUS.EASE.XQ: 0-100 rank percentile, higher = better
            biz_raw = float(biz_rows[0]["value"])
            biz_val = round(biz_raw, 2)
            scores.append(100 - np.clip(biz_raw, 0, 100))

        score = float(np.clip(np.mean(scores) * 0.85, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "regulatory_quality_wgi": rq_val,
            "ease_of_business_score": biz_val,
            "interpretation": (
                "Very rigid regulatory environment: zoning adaptation severely constrained"
                if score > 65
                else "Moderate regulatory barriers to zoning flexibility"
                if score > 35
                else "Relatively flexible regulatory and zoning environment"
            ),
            "_sources": ["WGI:RQ.EST", "WDI:IC.BUS.EASE.XQ"],
        }
