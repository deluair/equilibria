"""Monetary Transmission Lag: speed of policy rate pass-through to lending rates.

Methodology
-----------
Cottarelli & Kourelis (1994) and Mojon (2000): interest rate pass-through varies
by country and financial structure. Fast, complete pass-through = effective
monetary transmission.

1. Pass-through coefficient: regress lending rate on policy rate
   lending_rate_t = alpha + beta * policy_rate_t + epsilon
   beta near 1 = full pass-through; beta < 1 = incomplete/sluggish

2. Speed: estimate lag using distributed lag model
   delta_lending_t = a + sum_{k=0}^{K} b_k * delta_policy_{t-k} + e
   Speed index = b_0 / sum(b_k) (fraction of adjustment in period 0)

3. Adjustment gap: current gap between lending rate and policy rate

Score reflects sluggishness and incompleteness:
  score = clip((1 - beta) * 60 + (1 - speed) * 40, 0, 100)
  Full, instant pass-through -> score 0 (STABLE)
  No pass-through -> score 100 (CRISIS)

Sources: WDI FR.INR.LEND (lending rate), IMF FIDR (policy rate)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MonetaryTransmissionLag(LayerBase):
    layer_id = "lMY"
    name = "Monetary Transmission Lag"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        lookback = kwargs.get("lookback_years", 15)

        lending_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FR.INR.LEND'
              AND dp.date >= date('now', ?)
            ORDER BY dp.date
            """,
            (country, f"-{lookback} years"),
        )

        policy_rows = await db.fetch_all(
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

        if not lending_rows or not policy_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient rate data"}

        lending_map = {r["date"]: float(r["value"]) for r in lending_rows}
        policy_map = {r["date"]: float(r["value"]) for r in policy_rows}
        common_dates = sorted(set(lending_map) & set(policy_map))

        if len(common_dates) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient overlapping data"}

        lending_arr = np.array([lending_map[d] for d in common_dates])
        policy_arr = np.array([policy_map[d] for d in common_dates])

        # Level pass-through: OLS lending on policy
        X = np.column_stack([np.ones(len(policy_arr)), policy_arr])
        beta_vec = np.linalg.lstsq(X, lending_arr, rcond=None)[0]
        beta = float(beta_vec[1])

        # Speed: first-difference model (lag 0 only if N is small)
        d_lending = np.diff(lending_arr)
        d_policy = np.diff(policy_arr)
        if len(d_policy) >= 3 and np.std(d_policy) > 1e-10:
            speed_coef = float(np.polyfit(d_policy, d_lending, 1)[0])
            speed_index = float(np.clip(speed_coef, 0, 1))
        else:
            speed_index = 0.5

        adjustment_gap = float(lending_arr[-1] - policy_arr[-1])

        score = float(np.clip((1.0 - min(max(beta, 0), 1)) * 60 + (1.0 - speed_index) * 40, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "pass_through_coefficient": round(beta, 3),
            "pass_through_complete": beta >= 0.9,
            "speed_index": round(speed_index, 3),
            "adjustment_gap_pp": round(adjustment_gap, 2),
            "lending_rate_latest": round(lending_arr[-1], 2),
            "policy_rate_latest": round(policy_arr[-1], 2),
            "n_obs": len(common_dates),
            "period": f"{common_dates[0]} to {common_dates[-1]}",
            "indicators": ["FR.INR.LEND", "FIDR"],
        }
