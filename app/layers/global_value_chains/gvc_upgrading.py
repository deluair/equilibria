"""GVC Upgrading module.

Measures product upgrading via high-technology exports growth trend.

GVC upgrading occurs when a country moves from low-value to high-value
activities within the chain. Rising high-tech export share is the most
direct observable indicator available in cross-country panel data.

Uses linear regression (scipy linregress) on TX.VAL.TECH.MF.ZS over time.
Negative slope = downgrading; positive slope = upgrading.

Score = clip(50 - slope * 200, 0, 100).
  slope = 0     -> score 50 (neutral)
  slope > 0.25  -> score ~0 (strong upgrading, low stress)
  slope < -0.25 -> score ~100 (active downgrading, crisis)

Sources: World Bank WDI (TX.VAL.TECH.MF.ZS).
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class GVCUpgrading(LayerBase):
    layer_id = "lVC"
    name = "GVC Upgrading"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'TX.VAL.TECH.MF.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient high-tech export data"}

        vals = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]
        x = np.arange(len(vals), dtype=float)

        slope, intercept, r_value, p_value, std_err = linregress(x, vals)

        score = float(np.clip(50.0 - slope * 200.0, 0.0, 100.0))

        # Five-year change if enough data
        five_yr_change = None
        if len(vals) >= 5:
            five_yr_change = round(float(vals[-1]) - float(vals[-5]), 2)

        return {
            "score": round(score, 1),
            "country": country,
            "hitech_trend_slope": round(float(slope), 5),
            "r_squared": round(float(r_value ** 2), 4),
            "p_value": round(float(p_value), 4),
            "mean_hitech_pct": round(float(np.mean(vals)), 2),
            "latest_hitech_pct": round(float(vals[-1]), 2),
            "five_yr_change_ppt": five_yr_change,
            "period": f"{dates[0]} to {dates[-1]}",
            "n_obs": len(vals),
            "interpretation": (
                "active GVC upgrading" if slope > 0.1
                else "stable GVC position" if slope >= -0.1
                else "GVC downgrading"
            ),
        }
