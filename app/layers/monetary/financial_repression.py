"""Financial Repression: negative real rates, credit controls, and government borrowing subsidy.

Methodology
-----------
McKinnon (1973) and Shaw (1973) defined financial repression as policies that
keep real interest rates artificially low (often negative) to:
  1. Reduce government debt service burden (implicit taxation of savers)
  2. Channel credit to preferred sectors
  3. Enable monetary financing of deficits at below-market rates

Reinhart & Sbrancia (2015) estimate that financial repression liquidated
government debt at 1-4% of GDP per year in the post-WWII period.

Measurement:
  real_rate = nominal_rate - expected_inflation
  proxy: FR.INR.RINR (World Bank real interest rate, % per year)

Financial repression indicators:
  - Persistently negative real interest rate (< 0)
  - Large negative real rate (< -5%) -> severe repression
  - Trend toward more negative rates -> intensifying repression

Scoring: negative real rates penalize savers and distort capital allocation.
  score = clip(-real_rate * 5 + 50, 0, 100)
  real_rate = +10% -> score 0 (no repression)
  real_rate = 0    -> score 50
  real_rate = -10% -> score 100 (severe repression)

Sources: World Bank WDI
  FR.INR.RINR  - Real interest rate (%)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FinancialRepression(LayerBase):
    layer_id = "l15"
    name = "Financial Repression"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 20)

        rows = await db.execute_fetchall(
            "SELECT date, value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE series_id = ?) "
            "AND date >= date('now', ?) ORDER BY date",
            (f"FR.INR.RINR_{country}", f"-{lookback} years"),
        )

        if not rows or len(rows) < 4:
            return {"score": 50.0, "results": {"error": "insufficient real interest rate data"}}

        dates = [r[0] for r in rows]
        real_rate = np.array([float(r[1]) for r in rows])
        n = len(real_rate)

        current = float(real_rate[-1])
        mean_rate = float(np.mean(real_rate))
        pct_negative = float(np.mean(real_rate < 0.0)) * 100.0

        # Consecutive negative years (persistence)
        consecutive = 0
        max_consecutive = 0
        for v in real_rate:
            if v < 0:
                consecutive += 1
                max_consecutive = max(max_consecutive, consecutive)
            else:
                consecutive = 0

        # Trend: is repression intensifying?
        t = np.arange(n, dtype=float)
        trend_slope = float(np.polyfit(t, real_rate, 1)[0])

        # Rolling 5-year mean
        rolling_mean = float(np.mean(real_rate[-5:])) if n >= 5 else mean_rate

        results: dict = {
            "country": country,
            "n_obs": n,
            "period": f"{dates[0]} to {dates[-1]}",
            "real_rate_latest_pct": round(current, 3),
            "real_rate_mean_pct": round(mean_rate, 3),
            "real_rate_5yr_mean_pct": round(rolling_mean, 3),
            "pct_years_negative": round(pct_negative, 1),
            "max_consecutive_negative_yrs": max_consecutive,
            "trend_slope_pct_yr": round(trend_slope, 4),
            "repression_intensifying": trend_slope < -0.5,
            "severe_repression": current < -5.0,
            "repression_level": (
                "severe" if rolling_mean < -5.0
                else "moderate" if rolling_mean < 0.0
                else "none"
            ),
        }

        # Score per spec: clip(-real_rate * 5 + 50, 0, 100) using rolling mean
        score = float(np.clip(-rolling_mean * 5.0 + 50.0, 0.0, 100.0))
        if max_consecutive >= 5:
            score = min(score + 10.0, 100.0)

        return {"score": round(score, 1), "results": results}
