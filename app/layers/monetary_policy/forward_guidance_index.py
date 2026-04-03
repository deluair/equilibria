"""Forward Guidance Index: clarity and consistency of forward guidance.

Methodology
-----------
Forward guidance effectiveness is proxied through outcomes observable in data:
  1. Short-term rate predictability: how well does the current rate predict
     next-period rate (Gurkaynak et al. 2005)?
     High predictability -> clear guidance
     R^2 from AR(1) of policy rate changes

  2. Rate volatility: high unexpected rate moves signal poor guidance
     vol = std(delta_policy_rate)

  3. Reversal frequency: how often does the central bank reverse direction?
     reversals = count of sign changes in delta_policy / n_periods

Score = clip(vol * 5 + reversal_rate * 100, 0, 100)
  Stable, predictable path -> score 0 (STABLE)
  Highly volatile, frequent reversals -> score 100 (CRISIS)

Sources: IMF FIDR (policy/discount rate)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ForwardGuidanceIndex(LayerBase):
    layer_id = "lMY"
    name = "Forward Guidance Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        lookback = kwargs.get("lookback_years", 10)

        rows = await db.fetch_all(
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

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient policy rate data"}

        rates = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]

        delta = np.diff(rates)
        vol = float(np.std(delta, ddof=1)) if len(delta) > 1 else 0.0

        # AR(1) R^2 as predictability measure
        if len(rates) > 4:
            ar1_coef = np.corrcoef(rates[:-1], rates[1:])[0, 1]
            predictability_r2 = float(ar1_coef ** 2)
        else:
            predictability_r2 = 0.5

        # Reversal frequency: sign changes in delta
        if len(delta) > 2:
            sign_changes = int(np.sum(np.diff(np.sign(delta[delta != 0])) != 0))
            reversal_rate = sign_changes / max(len(delta) - 1, 1)
        else:
            sign_changes = 0
            reversal_rate = 0.0

        rate_latest = float(rates[-1])
        rate_range = float(rates.max() - rates.min())

        score = float(np.clip(vol * 5.0 + reversal_rate * 100.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "policy_rate_latest": round(rate_latest, 2),
            "rate_change_volatility_pp": round(vol, 3),
            "ar1_predictability_r2": round(predictability_r2, 3),
            "reversal_frequency": round(reversal_rate, 3),
            "sign_changes": sign_changes,
            "rate_range_pp": round(rate_range, 2),
            "guidance_quality": (
                "clear" if score < 25
                else "moderate" if score < 50
                else "opaque"
            ),
            "n_obs": len(rows),
            "period": f"{dates[0]} to {dates[-1]}",
        }
