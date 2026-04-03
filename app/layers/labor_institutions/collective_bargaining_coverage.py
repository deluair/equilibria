"""Collective bargaining coverage: share of workers under collective agreements.

Collective bargaining coverage measures the proportion of employees whose pay
and conditions of employment are determined by collective agreements. Coverage
can exceed union density when agreements are extended by law to non-union workers
(erga omnes extensions), common in Continental Europe.

High coverage is associated with lower wage inequality, more coordinated
wage-setting, and macroeconomic stabilization benefits (Calmfors & Driffill 1988,
Visser 2016).

Scoring (higher coverage -> lower institutional stress):
    score = clip(100 - coverage_pct * 1.25, 0, 100)

    coverage = 80%  -> score = 0   (near-universal, very strong)
    coverage = 40%  -> score = 50
    coverage = 15%  -> score = 81  (fragmented)
    coverage = 0%   -> score = 100 (no collective bargaining)

Sources: ILOSTAT (CBC_LTUR_NOC_RT — collective bargaining coverage rate, %)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

SERIES = "CBC_LTUR_NOC_RT"


class CollectiveBargainingCoverage(LayerBase):
    layer_id = "lLI"
    name = "Collective Bargaining Coverage"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'CBC_LTUR_NOC_RT'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no collective bargaining data (CBC_LTUR_NOC_RT)",
            }

        latest_date = rows[0]["date"]
        coverage_pct = float(rows[0]["value"])

        score = float(np.clip(100.0 - coverage_pct * 1.25, 0.0, 100.0))

        if coverage_pct >= 70:
            coverage_level = "near-universal"
        elif coverage_pct >= 40:
            coverage_level = "high"
        elif coverage_pct >= 20:
            coverage_level = "moderate"
        else:
            coverage_level = "low"

        trend_direction = "insufficient data"
        recent = sorted(rows[:10], key=lambda r: r["date"])
        if len(recent) >= 3:
            vals = np.array([float(r["value"]) for r in recent], dtype=float)
            slope = float(np.polyfit(np.arange(len(vals), dtype=float), vals, 1)[0])
            trend_direction = "rising" if slope > 0.2 else "falling" if slope < -0.2 else "stable"

        return {
            "score": round(score, 2),
            "country": country,
            "coverage_pct": round(coverage_pct, 2),
            "coverage_level": coverage_level,
            "trend": trend_direction,
            "latest_date": latest_date,
            "n_obs": len(rows),
            "note": (
                "score = clip(100 - coverage_pct * 1.25, 0, 100). "
                "Series: CBC_LTUR_NOC_RT (ILOSTAT)."
            ),
        }
