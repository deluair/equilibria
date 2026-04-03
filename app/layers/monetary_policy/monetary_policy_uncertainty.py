"""Monetary Policy Uncertainty: MPU index from observable data proxies.

Methodology
-----------
Baker, Bloom & Davis (2016) EPU index and its monetary policy sub-component.
In absence of direct text-based MPU scores, construct from:

1. Policy rate volatility: unexpectedly large or frequent rate changes
   rate_vol = std(delta_policy_rate, rolling 4-period) -> high = uncertain

2. Inflation surprise: deviation of actual from expected inflation
   surprise = |actual_inflation - 1yr_ahead_forecast|
   High repeated surprises -> monetary policy not credibly controlling inflation

3. Interest rate forecast error (if expectations data available):
   fe = |actual_rate_{t+1} - expected_rate_t|

Score = clip(rate_vol * 10 + mean_inf_surprise * 5, 0, 100)
  Low volatility, low surprises -> score 0 (STABLE)
  High volatility and surprises -> score 100 (CRISIS)

Sources: IMF FIDR (policy rate), FP.CPI.TOTL.ZG (inflation),
         PCPIEPCH (1yr inflation expectations)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MonetaryPolicyUncertainty(LayerBase):
    layer_id = "lMY"
    name = "Monetary Policy Uncertainty"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        lookback = kwargs.get("lookback_years", 10)

        rate_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FIDR'
              AND dp.date >= date('now', ?)
            ORDER BY dp.date
            """,
            (country, f"-{lookback} years"),
        )

        inflation_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FP.CPI.TOTL.ZG'
              AND dp.date >= date('now', ?)
            ORDER BY dp.date
            """,
            (country, f"-{lookback} years"),
        )

        exp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'PCPIEPCH'
              AND dp.date >= date('now', ?)
            ORDER BY dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not rate_rows or len(rate_rows) < 4:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient policy rate data"}

        rates = np.array([float(r["value"]) for r in rate_rows])
        rate_dates = [r["date"] for r in rate_rows]
        delta_rates = np.diff(rates)
        rate_vol = float(np.std(delta_rates, ddof=1)) if len(delta_rates) > 1 else 0.0

        # Inflation surprise
        mean_inf_surprise: float = 0.0
        if inflation_rows and exp_rows and len(exp_rows) >= 3:
            inf_map = {r["date"]: float(r["value"]) for r in inflation_rows}
            exp_map = {r["date"]: float(r["value"]) for r in exp_rows}
            # Surprise in period t = actual_t - expectation_{t-1}
            exp_dates = sorted(exp_map)
            surprises = []
            for i in range(1, len(exp_dates)):
                prev_date = exp_dates[i - 1]
                curr_date = exp_dates[i]
                if curr_date in inf_map and prev_date in exp_map:
                    surprises.append(abs(inf_map[curr_date] - exp_map[prev_date]))
            if surprises:
                mean_inf_surprise = float(np.mean(surprises))

        reversal_count = 0
        if len(delta_rates) > 2:
            signs = np.sign(delta_rates[delta_rates != 0])
            if len(signs) > 1:
                reversal_count = int(np.sum(np.diff(signs) != 0))

        score = float(np.clip(rate_vol * 10.0 + mean_inf_surprise * 5.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "rate_change_volatility_pp": round(rate_vol, 3),
            "mean_inflation_surprise_pp": round(mean_inf_surprise, 3),
            "policy_rate_reversals": reversal_count,
            "policy_rate_latest": round(float(rates[-1]), 2),
            "uncertainty_level": (
                "low" if score < 25
                else "moderate" if score < 50
                else "high"
            ),
            "n_obs_rate": len(rate_rows),
            "period": f"{rate_dates[0]} to {rate_dates[-1]}",
        }
