"""Child labor prevalence and risk assessment.

Children in employment (ages 7-14) are a direct indicator of household poverty
and weak labor rights enforcement. Above 5% signals concern; above 20% signals
crisis conditions affecting human capital accumulation.

Scoring:
    score = clip(rate * 4, 0, 100)

    rate = 0%  -> score = 0   (no child labor)
    rate = 5%  -> score = 20  (concern threshold)
    rate = 12% -> score = 48
    rate = 20% -> score = 80  (crisis threshold)
    rate = 25% -> score = 100 (capped)

Sources: WDI (SL.TLF.0714.ZS — children in employment, ages 7-14, total %)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

SERIES = "SL.TLF.0714.ZS"


class ChildLabor(LayerBase):
    layer_id = "l3"
    name = "Child Labor"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.TLF.0714.ZS'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no child labor data"}

        latest_date = rows[0]["date"]
        rate = float(rows[0]["value"])

        score = float(np.clip(rate * 4.0, 0.0, 100.0))

        # Classify severity
        if rate >= 20:
            severity = "crisis"
        elif rate >= 10:
            severity = "high"
        elif rate >= 5:
            severity = "concern"
        else:
            severity = "low"

        # Trend from historical observations
        recent = sorted(rows[:10], key=lambda r: r["date"])
        trend_direction = "insufficient data"
        if len(recent) >= 3:
            vals = np.array([float(r["value"]) for r in recent], dtype=float)
            t_idx = np.arange(len(vals), dtype=float)
            slope = float(np.polyfit(t_idx, vals, 1)[0])
            trend_direction = "rising" if slope > 0.1 else "falling" if slope < -0.1 else "stable"

        return {
            "score": round(score, 2),
            "country": country,
            "child_labor_rate_pct": round(rate, 2),
            "severity": severity,
            "latest_date": latest_date,
            "trend": trend_direction,
            "n_obs": len(rows),
            "thresholds": {"concern_pct": 5, "crisis_pct": 20},
            "note": "score = clip(rate * 4, 0, 100). Series: SL.TLF.0714.ZS (ages 7-14)",
        }
