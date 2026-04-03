"""Labor force participation rate gap from benchmark.

Measures how far a country's overall LFPR sits below the 65% benchmark.
Low participation means a large share of working-age adults are neither
employed nor actively seeking work, reducing productive capacity.

Scoring:
    score = max(0, 65 - lfpr) * 1.54, clipped to 100

    lfpr = 65% -> score = 0   (at benchmark, no gap)
    lfpr = 55% -> score = 15.4
    lfpr = 40% -> score = 38.5
    lfpr = 0%  -> score = 100 (full participation collapse)

Note: The existing LaborForceParticipation module (labor_force.py) performs
full demographic decomposition. This module provides a simple, fast benchmark-
gap score suitable for composite scoring in the L3 layer.

Sources: WDI (SL.TLF.CACT.ZS — labor force participation rate, total, %)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

SERIES = "SL.TLF.CACT.ZS"
BENCHMARK = 65.0


class LaborForceParticipationGap(LayerBase):
    layer_id = "l3"
    name = "Labor Force Participation Gap"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.TLF.CACT.ZS'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no LFPR data"}

        latest_date = rows[0]["date"]
        lfpr = float(rows[0]["value"])

        gap = max(0.0, BENCHMARK - lfpr)
        score = float(np.clip(gap * 1.54, 0.0, 100.0))

        # Trend direction from recent history
        recent = sorted(rows[:10], key=lambda r: r["date"])
        trend_direction = "insufficient data"
        if len(recent) >= 4:
            vals = np.array([float(r["value"]) for r in recent], dtype=float)
            t_idx = np.arange(len(vals), dtype=float)
            slope = float(np.polyfit(t_idx, vals, 1)[0])
            trend_direction = "rising" if slope > 0.1 else "falling" if slope < -0.1 else "stable"

        return {
            "score": round(score, 2),
            "country": country,
            "lfpr_pct": round(lfpr, 2),
            "benchmark_pct": BENCHMARK,
            "gap_pp": round(gap, 2),
            "latest_date": latest_date,
            "trend": trend_direction,
            "n_obs": len(rows),
            "note": "score = clip(max(0, 65 - lfpr) * 1.54, 0, 100). Series: SL.TLF.CACT.ZS",
        }
