"""Confirmation Bias module.

Policy persistence bias: inflation regime persistence.
Inflation staying above 5% for 5+ consecutive years indicates policymakers
confirm their prior beliefs rather than adjusting -- confirmation bias in monetary policy.

Score derived from the longest streak of above-threshold inflation years.

Source: WDI FP.CPI.TOTL.ZG (Inflation, consumer prices, annual %)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

INFLATION_THRESHOLD = 5.0  # percent


class ConfirmationBias(LayerBase):
    layer_id = "l13"
    name = "Confirmation Bias"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FP.CPI.TOTL.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]

        # Find longest consecutive streak above threshold
        max_streak = 0
        current_streak = 0
        for v in values:
            if v > INFLATION_THRESHOLD:
                current_streak += 1
                max_streak = max(max_streak, current_streak)
            else:
                current_streak = 0

        n_above = int(np.sum(values > INFLATION_THRESHOLD))
        pct_above = float(n_above / len(values))

        # Score: 5-year streak = 50 points, 10-year = 100 points
        score = float(np.clip(max_streak * 10, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(values),
            "period": f"{dates[0]} to {dates[-1]}",
            "inflation_threshold_pct": INFLATION_THRESHOLD,
            "max_consecutive_years_above_threshold": max_streak,
            "years_above_threshold": n_above,
            "pct_years_above_threshold": round(pct_above, 3),
            "mean_inflation": round(float(np.mean(values)), 2),
            "interpretation": "Long streaks of above-target inflation indicate confirmation bias in policy",
        }
