"""Poverty Gap Depth module.

Measures how far below the poverty line the poor actually are, using the
World Bank poverty gap index at $2.15/day (SI.POV.GAPS). A high gap means
poverty is not only widespread but deep -- the poor are far from the line.

Score = clip(gap_pct * 8, 0, 100).

Sources: WDI (SI.POV.GAPS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PovertyGapDepth(LayerBase):
    layer_id = "lID"
    name = "Poverty Gap Depth"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.POV.GAPS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]

        gap_pct = float(values[-1])  # most recent observation
        gap_mean = float(np.mean(values))
        gap_min = float(np.min(values))
        gap_max = float(np.max(values))

        # Trend: is poverty gap shrinking or widening?
        t = np.arange(len(values))
        slope = float(np.polyfit(t, values, 1)[0]) if len(values) >= 3 else 0.0

        score = float(np.clip(gap_pct * 8, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(values),
            "period": f"{dates[0]} to {dates[-1]}",
            "poverty_gap_pct": round(gap_pct, 3),
            "poverty_gap_mean_pct": round(gap_mean, 3),
            "poverty_gap_min_pct": round(gap_min, 3),
            "poverty_gap_max_pct": round(gap_max, 3),
            "gap_trend_per_year": round(slope, 4),
            "poverty_line": "$2.15/day (2017 PPP)",
            "interpretation": (
                "gap % = mean shortfall of poor below poverty line as share of line; "
                "higher = deeper poverty"
            ),
        }
