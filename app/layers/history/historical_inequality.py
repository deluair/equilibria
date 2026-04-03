"""Historical Inequality module.

Fits a linear trend to the Gini coefficient over time. A rising Gini
indicates worsening income distribution, a historical stress signal.

Indicator: SI.POV.GINI (Gini index, WDI).
Method: scipy.stats.linregress on available annual observations.
Score: clip(slope * 500 + 50, 0, 100).
  - slope = 0   -> score 50 (neutral)
  - slope > 0.1 -> score 100 (rapidly worsening inequality)
  - slope < -0.1 -> score 0  (improving)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class HistoricalInequality(LayerBase):
    layer_id = "lHI"
    name = "Historical Inequality"

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

        years = np.array([float(r["date"][:4]) for r in rows])
        gini = np.array([float(r["value"]) for r in rows])

        slope, intercept, r_value, p_value, std_err = linregress(years, gini)

        score = float(np.clip(slope * 500 + 50, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(rows),
            "period": f"{rows[0]['date'][:4]} to {rows[-1]['date'][:4]}",
            "gini_slope": round(float(slope), 6),
            "gini_latest": round(float(gini[-1]), 2),
            "gini_mean": round(float(np.mean(gini)), 2),
            "r_squared": round(r_value ** 2, 4),
            "p_value": round(float(p_value), 4),
            "trend_direction": "rising" if slope > 0 else "falling",
        }
