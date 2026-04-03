"""Youth unemployment stress relative to adult unemployment.

Compares youth unemployment rate (ages 15-24) to the adult (total) rate.
A ratio above 2x is a well-documented indicator of labor market stress for
young workers: scarring effects, skill depreciation, and social costs rise
sharply when youth face disproportionate joblessness.

Scoring:
    ratio = youth_rate / adult_rate
    score = clip((ratio - 1.5) * 40, 0, 100)

    ratio = 1.5 -> score = 0 (no stress; 1.5x is near-normal)
    ratio = 2.0 -> score = 20
    ratio = 3.0 -> score = 60
    ratio = 4.0 -> score = 100

Sources: WDI (SL.UEM.1524.ZS youth unemployment, SL.UEM.TOTL.ZS total)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

YOUTH_SERIES = "SL.UEM.1524.ZS"
ADULT_SERIES = "SL.UEM.TOTL.ZS"


class YouthUnemployment(LayerBase):
    layer_id = "l3"
    name = "Youth Unemployment"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('SL.UEM.1524.ZS', 'SL.UEM.TOTL.ZS')
              AND dp.value IS NOT NULL
            ORDER BY ds.series_id, dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no unemployment data"}

        # Collect latest value per series
        latest: dict[str, float] = {}
        history: dict[str, list[tuple[str, float]]] = {}
        for r in rows:
            sid = r["series_id"]
            val = float(r["value"])
            if sid not in latest:
                latest[sid] = val
            history.setdefault(sid, []).append((r["date"], val))

        if YOUTH_SERIES not in latest or ADULT_SERIES not in latest:
            missing = []
            if YOUTH_SERIES not in latest:
                missing.append(YOUTH_SERIES)
            if ADULT_SERIES not in latest:
                missing.append(ADULT_SERIES)
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"missing series: {', '.join(missing)}",
            }

        youth_rate = latest[YOUTH_SERIES]
        adult_rate = latest[ADULT_SERIES]

        if adult_rate <= 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "adult unemployment rate is zero or negative"}

        ratio = youth_rate / adult_rate
        score = float(np.clip((ratio - 1.5) * 40.0, 0.0, 100.0))

        # Trend: direction of youth rate over available history
        youth_hist = sorted(history.get(YOUTH_SERIES, []), key=lambda x: x[0])
        trend_direction = "insufficient data"
        if len(youth_hist) >= 4:
            vals = np.array([v for _, v in youth_hist[-8:]], dtype=float)
            t_idx = np.arange(len(vals), dtype=float)
            slope = float(np.polyfit(t_idx, vals, 1)[0])
            trend_direction = "rising" if slope > 0.1 else "falling" if slope < -0.1 else "stable"

        return {
            "score": round(score, 2),
            "country": country,
            "youth_unemployment_rate": round(youth_rate, 2),
            "adult_unemployment_rate": round(adult_rate, 2),
            "youth_to_adult_ratio": round(ratio, 3),
            "trend_youth_rate": trend_direction,
            "n_obs": len(rows),
            "note": (
                "Ratio > 2x = labor market stress for youth. "
                "score = clip((ratio - 1.5) * 40, 0, 100)"
            ),
        }
