"""Currency Substitution: dollarization proxy via broad money volatility.

Methodology
-----------
When confidence in the domestic currency collapses, agents substitute toward
foreign currency. This reduces the domestic money supply and raises its
variability. Calvo & Vegh (1992) show that currency substitution is reflected
in the coefficient of variation (CV) of the domestic money-to-GDP ratio:

    CV = std(M2/GDP) / mean(M2/GDP)

High CV indicates:
  - Erratic money demand (flight episodes)
  - Dollarization pressure
  - Impaired monetary transmission

Additional signal: rolling window CV trend -- if CV is increasing, substitution
risk is growing.

Score: clip(CV * 200, 0, 100)
  CV = 0.05 -> score 10 (stable)
  CV = 0.25 -> score 50 (watch)
  CV = 0.50 -> score 100 (crisis)

Sources: World Bank WDI
  FM.LBL.BMNY.GD.ZS  - Broad money (% of GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CurrencySubstitution(LayerBase):
    layer_id = "l15"
    name = "Currency Substitution"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 20)
        window = kwargs.get("rolling_window", 5)

        rows = await db.execute_fetchall(
            "SELECT date, value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE series_id = ?) "
            "AND date >= date('now', ?) ORDER BY date",
            (f"FM.LBL.BMNY.GD.ZS_{country}", f"-{lookback} years"),
        )

        if not rows or len(rows) < 5:
            return {"score": 50.0, "results": {"error": "insufficient broad money data"}}

        dates = [r[0] for r in rows]
        values = np.array([float(r[1]) for r in rows])

        n = len(values)
        mean_val = float(np.mean(values))
        std_val = float(np.std(values, ddof=1))
        cv_full = std_val / abs(mean_val) if abs(mean_val) > 1e-6 else 0.0

        # Rolling CV trend
        rolling_cvs: list[float] = []
        if n >= window:
            for i in range(window, n + 1):
                seg = values[i - window:i]
                m = float(np.mean(seg))
                s = float(np.std(seg, ddof=1)) if len(seg) > 1 else 0.0
                rolling_cvs.append(s / abs(m) if abs(m) > 1e-6 else 0.0)

        cv_trend_slope: float | None = None
        if len(rolling_cvs) >= 3:
            t = np.arange(len(rolling_cvs), dtype=float)
            cv_trend_slope = float(np.polyfit(t, rolling_cvs, 1)[0])

        results: dict = {
            "country": country,
            "n_obs": n,
            "period": f"{dates[0]} to {dates[-1]}",
            "broad_money_gdp_mean_pct": round(mean_val, 3),
            "broad_money_gdp_std_pct": round(std_val, 3),
            "coefficient_of_variation": round(cv_full, 4),
            "rolling_window": window,
            "rolling_cv_latest": round(rolling_cvs[-1], 4) if rolling_cvs else None,
            "cv_trend_slope": round(cv_trend_slope, 6) if cv_trend_slope is not None else None,
            "cv_increasing": cv_trend_slope > 0 if cv_trend_slope is not None else None,
            "substitution_risk_level": (
                "high" if cv_full > 0.40
                else "moderate" if cv_full > 0.20
                else "low"
            ),
        }

        score = float(np.clip(cv_full * 200.0, 0.0, 100.0))

        return {"score": round(score, 1), "results": results}
