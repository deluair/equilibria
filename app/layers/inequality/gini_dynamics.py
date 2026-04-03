"""Gini Dynamics module.

Tracks Gini coefficient trend and acceleration over time.

1. **Level**: Latest Gini coefficient value (0-100 scale, World Bank).
2. **Trend**: OLS linear regression slope over available years (linregress).
3. **Acceleration**: Second derivative — change in trend across two sub-periods.
   Rising Gini = distributional stress.

Score reflects how far Gini has risen and how fast it is accelerating.
Rising trend and positive acceleration each contribute to stress score.

Sources: WDI indicator SI.POV.GINI
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class GiniDynamics(LayerBase):
    layer_id = "lIQ"
    name = "Gini Dynamics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
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

        if not rows or len(rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        dates = [r["date"] for r in rows]
        values = np.array([float(r["value"]) for r in rows])
        years = np.array([float(d[:4]) for d in dates])

        # Level
        gini_latest = float(values[-1])

        # Trend via linregress
        slope, intercept, r_value, p_value, se = linregress(years, values)

        # Acceleration: compare slope of first half vs second half
        mid = len(values) // 2
        if mid >= 2 and len(values) - mid >= 2:
            slope_first, *_ = linregress(years[:mid], values[:mid])
            slope_second, *_ = linregress(years[mid:], values[mid:])
            acceleration = float(slope_second - slope_first)
        else:
            acceleration = 0.0

        # Score: base from level (Gini 30 = neutral, 60 = max)
        level_score = float(np.clip((gini_latest - 25.0) / 35.0 * 50.0, 0, 50))
        # Trend penalty: rising slope adds up to 30 points
        trend_score = float(np.clip(slope * 10.0, 0, 30))
        # Acceleration penalty: positive acceleration adds up to 20 points
        accel_score = float(np.clip(acceleration * 15.0, 0, 20))
        score = float(np.clip(level_score + trend_score + accel_score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(values),
            "period": f"{dates[0]} to {dates[-1]}",
            "gini_latest": round(gini_latest, 2),
            "trend_slope_per_year": round(float(slope), 4),
            "trend_r_squared": round(float(r_value**2), 4),
            "trend_p_value": round(float(p_value), 4),
            "acceleration": round(acceleration, 4),
            "interpretation": {
                "rising": slope > 0,
                "accelerating": acceleration > 0,
            },
        }
