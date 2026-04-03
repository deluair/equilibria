"""Labor regulation quality: regulatory quality for labor markets.

Regulatory quality captures the government's capacity to formulate and implement
sound policies and regulations that promote private sector development, including
in the labor market. The World Bank's Regulatory Quality indicator (RQ.EST) from
the WGI is the primary proxy.

A high regulatory quality score indicates an environment where labor laws are
consistently enforced, firms can plan with certainty, and workers have accessible
recourse mechanisms. Low regulatory quality correlates with corruption in labor
inspection, arbitrary enforcement, and weak worker protections in practice.

Scoring:
    normalized = (rq + 2.5) / 5.0
    score = clip((1 - normalized) * 100, 0, 100)

    rq = +2.5 -> score = 0   (excellent regulatory quality)
    rq =  0.0 -> score = 50
    rq = -2.5 -> score = 100 (very poor regulatory quality)

Sources: WDI (WGI series RQ.EST — regulatory quality estimate)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

SERIES = "RQ.EST"


class LaborRegulationQuality(LayerBase):
    layer_id = "lLI"
    name = "Labor Regulation Quality"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'RQ.EST'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no regulatory quality data (RQ.EST)",
            }

        latest_date = rows[0]["date"]
        rq = float(rows[0]["value"])

        normalized = (rq + 2.5) / 5.0
        score = float(np.clip((1.0 - normalized) * 100.0, 0.0, 100.0))

        if rq >= 1.0:
            quality = "excellent"
        elif rq >= 0.0:
            quality = "good"
        elif rq >= -1.0:
            quality = "poor"
        else:
            quality = "very poor"

        trend_direction = "insufficient data"
        recent = sorted(rows[:10], key=lambda r: r["date"])
        if len(recent) >= 3:
            vals = np.array([float(r["value"]) for r in recent], dtype=float)
            slope = float(np.polyfit(np.arange(len(vals), dtype=float), vals, 1)[0])
            trend_direction = "rising" if slope > 0.02 else "falling" if slope < -0.02 else "stable"

        return {
            "score": round(score, 2),
            "country": country,
            "regulatory_quality_est": round(rq, 4),
            "quality_level": quality,
            "trend": trend_direction,
            "latest_date": latest_date,
            "n_obs": len(rows),
            "note": (
                "score = clip((1 - (rq + 2.5) / 5) * 100, 0, 100). "
                "WGI scale -2.5 to +2.5. Series: RQ.EST."
            ),
        }
