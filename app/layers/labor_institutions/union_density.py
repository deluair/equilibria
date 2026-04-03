"""Trade union membership rate (union density).

Union density is the share of wage and salary earners who are members of a trade
union. It measures the organizational strength of labor and the capacity of
workers to collectively bargain over wages and conditions.

High union density is associated with compressed wage distributions, higher
bargaining power, and greater political influence for labor. Declining density
weakens the institutional underpinning of coordinated wage-setting.

Scoring (higher density -> lower institutional stress):
    score = clip(100 - density_pct * 2, 0, 100)

    density = 50%  -> score = 0   (very strong unions, no stress)
    density = 25%  -> score = 50
    density = 10%  -> score = 80  (weak)
    density = 0%   -> score = 100 (no union coverage, high stress)

Sources: ILOSTAT (TUD_LTUR_NOC_RT — trade union density rate, %)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

SERIES = "TUD_LTUR_NOC_RT"


class UnionDensity(LayerBase):
    layer_id = "lLI"
    name = "Union Density"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'TUD_LTUR_NOC_RT'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no union density data (TUD_LTUR_NOC_RT)",
            }

        latest_date = rows[0]["date"]
        density_pct = float(rows[0]["value"])

        score = float(np.clip(100.0 - density_pct * 2.0, 0.0, 100.0))

        if density_pct >= 40:
            level = "very high"
        elif density_pct >= 20:
            level = "high"
        elif density_pct >= 10:
            level = "moderate"
        else:
            level = "low"

        trend_direction = "insufficient data"
        recent = sorted(rows[:10], key=lambda r: r["date"])
        if len(recent) >= 3:
            vals = np.array([float(r["value"]) for r in recent], dtype=float)
            slope = float(np.polyfit(np.arange(len(vals), dtype=float), vals, 1)[0])
            trend_direction = "rising" if slope > 0.2 else "falling" if slope < -0.2 else "stable"

        return {
            "score": round(score, 2),
            "country": country,
            "union_density_pct": round(density_pct, 2),
            "density_level": level,
            "trend": trend_direction,
            "latest_date": latest_date,
            "n_obs": len(rows),
            "note": "score = clip(100 - density_pct * 2, 0, 100). Series: TUD_LTUR_NOC_RT",
        }
