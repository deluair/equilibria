"""Interest rate spread analysis.

The lending-deposit spread (FR.INR.LNDP) measures financial sector efficiency.
A wide spread reflects high intermediation costs, weak competition, elevated
credit risk premia, or macroeconomic instability -- all financial stress signals.

Score (0-100): clip(spread * 5, 0, 100).
High spread pushes toward CRISIS.
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class InterestRateSpread(LayerBase):
    layer_id = "l7"
    name = "Interest Rate Spread"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 10)

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.indicator_code = 'FR.INR.LNDP'
              AND ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
            ORDER BY dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not rows or len(rows) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient interest rate spread data",
            }

        values = np.array([float(r["value"]) for r in rows])
        spread_latest = float(values[-1])
        spread_mean = float(np.mean(values))
        spread_std = float(np.std(values, ddof=1)) if len(values) > 1 else 0.0

        score = float(np.clip(spread_latest * 5.0, 0.0, 100.0))

        # Trend
        trend = None
        if len(values) >= 3:
            x = np.arange(len(values))
            slope, _, r_val, _, _ = sp_stats.linregress(x, values)
            trend = {
                "slope_per_year": round(float(slope), 4),
                "r_squared": round(float(r_val ** 2), 4),
                "direction": "widening" if slope > 0.1 else "narrowing" if slope < -0.1 else "stable",
            }

        return {
            "score": round(score, 2),
            "country": country,
            "spread_pct": {
                "latest": round(spread_latest, 3),
                "mean": round(spread_mean, 3),
                "std_dev": round(spread_std, 3),
                "observations": len(values),
            },
            "efficiency_rating": (
                "inefficient" if spread_latest > 10
                else "below_average" if spread_latest > 5
                else "moderate" if spread_latest > 3
                else "efficient"
            ),
            "trend": trend,
            "indicator": "FR.INR.LNDP",
        }
