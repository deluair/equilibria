"""Historical Trade Patterns module.

Measures commodity lock-in via the fuel export share of merchandise exports.
A high or rising fuel export share indicates Dutch disease risk: resource
dependence crowds out manufacturing and services, leaving the economy
vulnerable to commodity price cycles.

Indicator: TX.VAL.FUEL.ZS.UN (Fuel exports, % of merchandise exports, WDI).
Score: clip(latest_fuel_share * 1.5, 0, 100).
  - 0%   -> 0   (diversified)
  - 33%  -> 50  (moderate dependence)
  - 67%  -> 100 (full commodity lock-in)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class HistoricalTradePatterns(LayerBase):
    layer_id = "lHI"
    name = "Historical Trade Patterns"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'TX.VAL.FUEL.ZS.UN'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = np.array([float(r["value"]) for r in rows])
        latest_value = float(values[-1])
        score = float(np.clip(latest_value * 1.5, 0, 100))

        trend_slope = None
        r_squared = None
        if len(rows) >= 5:
            years = np.array([float(r["date"][:4]) for r in rows])
            slope, _, r_value, _, _ = linregress(years, values)
            trend_slope = round(float(slope), 4)
            r_squared = round(r_value ** 2, 4)

        return {
            "score": round(score, 1),
            "country": country,
            "fuel_export_share_pct": round(latest_value, 2),
            "latest_year": rows[-1]["date"][:4],
            "n_obs": len(rows),
            "trend_slope_pct_per_yr": trend_slope,
            "r_squared": r_squared,
            "dutch_disease_risk": latest_value > 33,
        }
