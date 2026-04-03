"""Rule of Law module.

Measures the depth of rule of law and its trend over time.

Indicator: RL.EST (Rule of Law, World Bank WGI, scale -2.5 to +2.5).

Score formula:
  base  = clip(50 - rl_latest * 20, 0, 100)
  trend penalty applied if rule of law is declining (negative slope over
  the available history). Penalty capped at 15 points.

Sources: World Bank WDI (RL.EST)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class RuleOfLaw(LayerBase):
    layer_id = "lGV"
    name = "Rule of Law"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'RL.EST'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 1:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]
        rl_latest = float(values[-1])

        base_score = float(np.clip(50.0 - rl_latest * 20.0, 0.0, 100.0))

        # Trend analysis
        trend_penalty = 0.0
        slope = None
        if len(values) >= 3:
            x = np.arange(len(values), dtype=float)
            result = linregress(x, values)
            slope = float(result.slope)
            if slope < 0:
                # Declining rule of law amplifies stress
                trend_penalty = min(15.0, abs(slope) * 50.0)

        score = float(np.clip(base_score + trend_penalty, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "rl_latest": round(rl_latest, 4),
            "n_obs": len(values),
            "period": f"{dates[0]} to {dates[-1]}",
            "trend_slope": round(slope, 6) if slope is not None else None,
            "trend_penalty": round(trend_penalty, 2),
            "declining": slope is not None and slope < 0,
            "note": "RL.EST scale: -2.5 (worst) to +2.5 (best)",
        }
