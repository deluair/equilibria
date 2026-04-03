"""Labor dispute intensity: strike frequency and duration index.

Industrial disputes — strikes and lockouts — reflect the degree of conflict
between labor and capital over wages and working conditions. High dispute
intensity signals institutional breakdown in collective bargaining, elevated
uncertainty for firms, and potential productivity losses.

The ILO collects days not worked per 1,000 employees as the primary measure
(IDA_LTUR_NOC_RT). Higher values indicate more severe industrial conflict.

Scoring:
    Benchmark: >100 days/1,000 workers is high conflict.
    score = clip(days_per_1000 / 2, 0, 100)

    days = 0   -> score = 0   (no disputes)
    days = 50  -> score = 25
    days = 100 -> score = 50  (high)
    days = 200 -> score = 100 (crisis level)

Sources: ILOSTAT (IDA_LTUR_NOC_RT — days not worked per 1,000 employees)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

SERIES = "IDA_LTUR_NOC_RT"


class LaborDisputeIntensity(LayerBase):
    layer_id = "lLI"
    name = "Labor Dispute Intensity"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IDA_LTUR_NOC_RT'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no labor dispute data (IDA_LTUR_NOC_RT)",
            }

        latest_date = rows[0]["date"]
        days_per_1000 = float(rows[0]["value"])

        score = float(np.clip(days_per_1000 / 2.0, 0.0, 100.0))

        if days_per_1000 >= 200:
            intensity = "crisis"
        elif days_per_1000 >= 100:
            intensity = "high"
        elif days_per_1000 >= 30:
            intensity = "moderate"
        else:
            intensity = "low"

        trend_direction = "insufficient data"
        recent = sorted(rows[:10], key=lambda r: r["date"])
        if len(recent) >= 3:
            vals = np.array([float(r["value"]) for r in recent], dtype=float)
            slope = float(np.polyfit(np.arange(len(vals), dtype=float), vals, 1)[0])
            trend_direction = "rising" if slope > 1.0 else "falling" if slope < -1.0 else "stable"

        return {
            "score": round(score, 2),
            "country": country,
            "days_not_worked_per_1000": round(days_per_1000, 2),
            "dispute_intensity": intensity,
            "trend": trend_direction,
            "latest_date": latest_date,
            "n_obs": len(rows),
            "note": (
                "score = clip(days_per_1000 / 2, 0, 100). "
                "Series: IDA_LTUR_NOC_RT (days not worked per 1,000 employees)."
            ),
        }
