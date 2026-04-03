"""Credit-to-GDP Gap: Basel III early warning indicator for financial overheating.

Methodology
-----------
The Basel III countercyclical capital buffer (CCyB) framework uses the credit-to-GDP
gap as its primary indicator (BCBS, 2010):

    gap_t = (credit/GDP)_t - trend_t

where trend is estimated via a one-sided Hodrick-Prescott filter (lambda=400,000
for quarterly; 1,600 for annual data per Basel III guidance).

    HP objective: min_{trend} sum[(y_t - trend_t)^2 + lambda * sum(delta^2 trend_t)]

A positive gap above 2 pp signals rising systemic risk; above 10 pp triggers
the maximum CCyB add-on.

Drehmann & Tsatsaronis (2014) show the credit-to-GDP gap is the best single
early warning indicator with a 1-5 year horizon before banking crises.

Score = clip(max(0, gap) * 4, 0, 100)
  gap = 0   -> score 0 (stable)
  gap = 10  -> score 40 (watch)
  gap = 25  -> score 100 (crisis)

Sources: World Bank WDI
  FS.AST.DOMS.GD.ZS  - Domestic credit provided by financial sector (% of GDP)

References:
    BCBS (2010). Guidance for national authorities operating the countercyclical
        capital buffer. Bank for International Settlements.
    Drehmann, M. & Tsatsaronis, K. (2014). The credit-to-GDP gap and countercyclical
        capital buffers. BIS Quarterly Review, March 2014.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


def _hp_filter_trend(y: np.ndarray, lam: float = 1600.0) -> np.ndarray:
    """One-sided HP filter via matrix solution (Whittaker smoother)."""
    n = len(y)
    if n < 4:
        return y.copy()

    # Build second-difference matrix
    D = np.zeros((n - 2, n))
    for i in range(n - 2):
        D[i, i] = 1.0
        D[i, i + 1] = -2.0
        D[i, i + 2] = 1.0

    I = np.eye(n)
    A = I + lam * (D.T @ D)
    trend = np.linalg.solve(A, y)
    return trend


class CreditToGDPGap(LayerBase):
    layer_id = "l15"
    name = "Credit-to-GDP Gap"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 30)
        hp_lambda = kwargs.get("hp_lambda", 1600.0)

        rows = await db.execute_fetchall(
            "SELECT date, value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE series_id = ?) "
            "AND date >= date('now', ?) ORDER BY date",
            (f"FS.AST.DOMS.GD.ZS_{country}", f"-{lookback} years"),
        )

        if not rows or len(rows) < 8:
            return {"score": 50.0, "results": {"error": "insufficient credit/GDP data"}}

        dates = [r[0] for r in rows]
        values = np.array([float(r[1]) for r in rows])

        trend = _hp_filter_trend(values, lam=hp_lambda)
        gap = values - trend

        current_gap = float(gap[-1])
        current_credit_gdp = float(values[-1])
        current_trend = float(trend[-1])

        results: dict = {
            "country": country,
            "n_obs": len(dates),
            "period": f"{dates[0]} to {dates[-1]}",
            "hp_lambda": hp_lambda,
            "credit_gdp_latest_pct": round(current_credit_gdp, 3),
            "hp_trend_latest_pct": round(current_trend, 3),
            "gap_pp": round(current_gap, 3),
            "gap_5yr_max_pp": round(float(np.max(gap[-5:])), 3) if len(gap) >= 5 else None,
            "gap_mean_pp": round(float(np.mean(gap)), 3),
            "overheating": current_gap > 2.0,
            "severe_overheating": current_gap > 10.0,
            "bcbs_ccyb_signal": current_gap > 2.0,
        }

        # Score per spec: clip(max(0, gap) * 4, 0, 100)
        score = float(np.clip(max(0.0, current_gap) * 4.0, 0.0, 100.0))

        return {"score": round(score, 1), "results": results}
