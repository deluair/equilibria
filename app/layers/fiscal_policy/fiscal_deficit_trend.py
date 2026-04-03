"""Fiscal Deficit Trend module.

Estimates the trajectory of the fiscal balance (cash surplus/deficit as % of GDP).
A persistently widening deficit signals deteriorating fiscal sustainability and
growing macroeconomic stress.

Methodology:
- Query WDI indicator GC.BAL.CASH.GD.ZS (fiscal balance, % GDP).
- Compute level and linear trend (scipy.stats.linregress).
- Persistent widening (negative balance + negative slope) raises score.
- Score = clip(-balance * 6 + max(0, -slope) * 30, 0, 100).

Sources: World Bank WDI (GC.BAL.CASH.GD.ZS)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class FiscalDeficitTrend(LayerBase):
    layer_id = "lFP"
    name = "Fiscal Deficit Trend"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.BAL.CASH.GD.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]

        # Latest fiscal balance level
        balance = float(values[-1])

        # Linear trend over available history
        x = np.arange(len(values), dtype=float)
        slope, intercept, r_value, p_value, std_err = linregress(x, values)

        # Score: deficit (negative balance) raises score; worsening trend adds more
        score = float(np.clip(-balance * 6 + max(0.0, -slope) * 30, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "fiscal_balance_pct_gdp": round(balance, 3),
            "trend_slope": round(float(slope), 4),
            "trend_r2": round(float(r_value ** 2), 4),
            "trend_p_value": round(float(p_value), 4),
            "n_obs": len(values),
            "period": f"{dates[0]} to {dates[-1]}",
            "indicator": "GC.BAL.CASH.GD.ZS",
        }
