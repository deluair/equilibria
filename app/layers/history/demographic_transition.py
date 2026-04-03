"""Demographic Transition module.

Scores the age dependency ratio stage. Extremes in either direction
indicate stress: very high dependency (pre-dividend, high youth burden)
or very low dependency (post-dividend, ageing crisis) both constrain
growth. The sweet spot (demographic dividend) is around 55.

Indicator: SP.POP.DPND (Age dependency ratio, % of working-age population, WDI).
Score: abs(ratio - 55) * 1.5, clipped to [0, 100].
  - ratio = 55 -> score 0  (peak dividend)
  - ratio = 35 or 88 -> score ~30 or ~50
  - ratio > 122 or < -12 -> score 100 (extreme, saturated)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_OPTIMAL_RATIO = 55.0
_SCALE = 1.5


class DemographicTransition(LayerBase):
    layer_id = "lHI"
    name = "Demographic Transition"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.POP.DPND'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        latest_ratio = float(rows[0]["value"])
        deviation = abs(latest_ratio - _OPTIMAL_RATIO)
        score = float(np.clip(deviation * _SCALE, 0, 100))

        stage = "pre-dividend" if latest_ratio > 75 else (
            "dividend" if latest_ratio <= 75 and latest_ratio >= 45 else "post-dividend"
        )

        return {
            "score": round(score, 1),
            "country": country,
            "dependency_ratio": round(latest_ratio, 2),
            "optimal_ratio": _OPTIMAL_RATIO,
            "deviation_from_optimal": round(deviation, 2),
            "stage": stage,
            "latest_year": rows[0]["date"][:4],
        }
