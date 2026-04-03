"""Real Money Demand Stability: M2/GDP ratio variance and monetary transmission quality.

Methodology
-----------
Stable money demand is a prerequisite for rules-based monetary policy.
Lucas (1988) and Stock & Watson (1993) show that instability in M2 velocity
during the 1980s-90s was caused by financial innovation shifting the demand function.

Stability metrics:
  1. Coefficient of variation (CV) of M2/GDP ratio
  2. Variance ratio test (Lo-MacKinlay 1988): var(k-period returns) / (k * var(1-period returns))
     Under random walk (unstable demand): ratio -> 1
     Under mean-reversion (stable demand): ratio < 1
  3. Linear trend in M2/GDP: large R-squared with consistent trend = predictable
  4. Structural break: rolling variance increase signals regime shift

Score: high variance = unstable money demand = stress.
    cv > 0.30 -> severe instability
    score = clip(cv * 250, 0, 100)

Sources: World Bank WDI
  FM.LBL.BMNY.GD.ZS  - Broad money (% of GDP)
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class RealMoneyDemand(LayerBase):
    layer_id = "l15"
    name = "Real Money Demand Stability"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 25)
        vr_lag = kwargs.get("variance_ratio_lag", 4)

        rows = await db.execute_fetchall(
            "SELECT date, value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE series_id = ?) "
            "AND date >= date('now', ?) ORDER BY date",
            (f"FM.LBL.BMNY.GD.ZS_{country}", f"-{lookback} years"),
        )

        if not rows or len(rows) < 8:
            return {"score": 50.0, "results": {"error": "insufficient broad money data"}}

        dates = [r[0] for r in rows]
        values = np.array([float(r[1]) for r in rows])
        n = len(values)

        mean_val = float(np.mean(values))
        std_val = float(np.std(values, ddof=1))
        cv = std_val / abs(mean_val) if abs(mean_val) > 1e-6 else 0.0

        # Linear trend R-squared
        t = np.arange(n, dtype=float)
        slope, intercept, r, p, se = sp_stats.linregress(t, values)
        trend_r2 = float(r ** 2)

        # Variance ratio (Lo-MacKinlay)
        vr: float | None = None
        if n >= vr_lag * 4:
            changes = np.diff(values)
            var1 = float(np.var(changes, ddof=1))
            if var1 > 1e-10 and n > vr_lag:
                k_changes = values[vr_lag:] - values[:-vr_lag]
                var_k = float(np.var(k_changes, ddof=1))
                vr = var_k / (vr_lag * var1)

        # Rolling variance to detect regime shift
        half = n // 2
        var_first = float(np.var(values[:half], ddof=1)) if half > 1 else 0.0
        var_second = float(np.var(values[half:], ddof=1)) if (n - half) > 1 else 0.0
        variance_increasing = var_second > var_first * 1.5 if var_first > 1e-10 else False

        results: dict = {
            "country": country,
            "n_obs": n,
            "period": f"{dates[0]} to {dates[-1]}",
            "m2_gdp_mean_pct": round(mean_val, 3),
            "m2_gdp_std_pct": round(std_val, 3),
            "coefficient_of_variation": round(cv, 4),
            "trend_r_squared": round(trend_r2, 4),
            "trend_slope_pct_yr": round(float(slope), 4),
            "variance_ratio": round(vr, 4) if vr is not None else None,
            "mean_reverting": vr < 0.9 if vr is not None else None,
            "variance_increasing": variance_increasing,
            "stability_level": (
                "stable" if cv < 0.10
                else "moderate" if cv < 0.25
                else "unstable"
            ),
        }

        score = float(np.clip(cv * 250.0, 0.0, 100.0))
        if variance_increasing:
            score = min(score + 15.0, 100.0)

        return {"score": round(score, 1), "results": results}
