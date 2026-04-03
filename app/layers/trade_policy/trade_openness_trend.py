"""Trade Openness Trend module.

Measures the trajectory of trade liberalization by fitting a linear trend
to trade openness (trade as % of GDP) over time. A declining trend signals
a protectionist shift.

Score = clip(50 - slope * 30, 0, 100)

Sources: WDI
  NE.TRD.GNFS.ZS - Trade (% of GDP)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class TradeOpennessTrend(LayerBase):
    layer_id = "lTP"
    name = "Trade Openness Trend"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.TRD.GNFS.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient trade openness data"}

        valid = [(r["date"], float(r["value"])) for r in rows if r["value"] is not None]
        if len(valid) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid observations"}

        dates = [v[0] for v in valid]
        values = np.array([v[1] for v in valid])
        t = np.arange(len(values), dtype=float)

        slope, intercept, r_value, p_value, std_err = linregress(t, values)

        score = float(np.clip(50 - slope * 30, 0, 100))

        trajectory = (
            "liberalizing" if slope > 0.5
            else "stable" if abs(slope) <= 0.5
            else "protectionist drift"
        )

        return {
            "score": round(score, 1),
            "country": country,
            "openness_slope_pct_per_year": round(float(slope), 4),
            "mean_openness_pct_gdp": round(float(np.mean(values)), 2),
            "latest_openness_pct_gdp": round(float(values[-1]), 2),
            "r_squared": round(float(r_value**2), 4),
            "p_value": round(float(p_value), 4),
            "trajectory": trajectory,
            "period": f"{dates[0]} to {dates[-1]}",
            "n_obs": len(values),
        }
