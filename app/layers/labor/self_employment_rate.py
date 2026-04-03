"""Self-employment rate as a proxy for labor market informality and precariousness.

High self-employment share is a well-established indicator of informality,
particularly in developing economies where workers are own-account operators
rather than salaried employees. Above 50% is associated with limited social
protection, earnings volatility, and informality stress.

Scoring:
    score = clip((rate - 20) * 1.25, 0, 100)

    rate = 20% -> score = 0   (baseline; typical of upper-middle income)
    rate = 40% -> score = 25
    rate = 60% -> score = 50
    rate = 100% -> score = 100

Sources: WDI (SL.EMP.SELF.ZS — self-employed, % of total employment)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

SERIES = "SL.EMP.SELF.ZS"


class SelfEmploymentRate(LayerBase):
    layer_id = "l3"
    name = "Self-Employment Rate"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.EMP.SELF.ZS'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no self-employment data"}

        # Latest observation
        latest_date = rows[0]["date"]
        rate = float(rows[0]["value"])

        score = float(np.clip((rate - 20.0) * 1.25, 0.0, 100.0))

        # Trend over last 5 available years
        recent = sorted(rows[:10], key=lambda r: r["date"])
        trend_direction = "insufficient data"
        if len(recent) >= 4:
            vals = np.array([float(r["value"]) for r in recent], dtype=float)
            t_idx = np.arange(len(vals), dtype=float)
            slope = float(np.polyfit(t_idx, vals, 1)[0])
            trend_direction = "rising" if slope > 0.2 else "falling" if slope < -0.2 else "stable"

        # Stress classification
        if rate >= 70:
            stress_level = "severe informality"
        elif rate >= 50:
            stress_level = "high informality"
        elif rate >= 35:
            stress_level = "moderate informality"
        else:
            stress_level = "low informality"

        return {
            "score": round(score, 2),
            "country": country,
            "self_employment_rate_pct": round(rate, 2),
            "latest_date": latest_date,
            "stress_level": stress_level,
            "trend": trend_direction,
            "n_obs": len(rows),
            "note": (
                "score = clip((rate - 20) * 1.25, 0, 100). "
                ">50% = informality stress. Series: SL.EMP.SELF.ZS"
            ),
        }
