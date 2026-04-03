"""Structural stability test: Chow test for structural break at midpoint.

Methodology
-----------
**Chow Test** (Chow 1960):
    H0: regression coefficients are equal across two subsamples
    H1: structural break (coefficients differ)

Procedure:
1. Estimate pooled OLS: y = X*beta + eps (T obs)
2. Estimate OLS in sub-period 1: y_1 = X_1*beta_1 + eps_1 (T_1 obs)
3. Estimate OLS in sub-period 2: y_2 = X_2*beta_2 + eps_2 (T_2 obs)

F-statistic:
    F = [(SSR_P - (SSR_1 + SSR_2)) / k] / [(SSR_1 + SSR_2) / (T - 2k)]
    where k = number of parameters (2: intercept + trend), T = total obs

Under H0: F ~ F(k, T - 2k)

Break point: midpoint of the series (T/2).

Large F-statistic = structural instability in GDP growth trend.

Score = clip(f_stat / 10 * 100, 0, 100)
    - F=0: score 0 (stable)
    - F=10: score 100 (unstable)

References:
    Chow, G.C. (1960). Tests of equality between sets of coefficients in two
        linear regressions. Econometrica 28(3): 591-605.
"""

import numpy as np
from scipy.stats import f as f_dist

from app.layers.base import LayerBase


def _ols_ssr(X: np.ndarray, y: np.ndarray) -> float:
    """Return SSR from OLS."""
    if len(y) < 2 or X.shape[0] < X.shape[1]:
        return float(np.sum(y ** 2))
    try:
        beta, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
        resid = y - X @ beta
        return float(np.sum(resid ** 2))
    except np.linalg.LinAlgError:
        return float(np.sum(y ** 2))


class StructuralStabilityTest(LayerBase):
    layer_id = "l18"
    name = "Structural Stability Test"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.indicator_code = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        dated = [(r["date"], float(r["value"])) for r in rows if r["value"] is not None]

        if len(dated) < 20:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data for Chow test"}

        dates, values = zip(*dated)
        y = np.array(values)
        n = len(y)
        t = np.arange(n, dtype=float)

        # Midpoint split
        mid = n // 2
        breakpoint_date = dates[mid]

        X = np.column_stack([np.ones(n), t])
        k = X.shape[1]

        # Pooled SSR
        ssr_p = _ols_ssr(X, y)

        # Sub-period SSRs
        y1, X1 = y[:mid], X[:mid]
        y2, X2 = y[mid:], X[mid:]

        # Ensure each sub-period has enough obs
        if len(y1) < k + 1 or len(y2) < k + 1:
            return {"score": None, "signal": "UNAVAILABLE", "error": "sub-periods too small"}

        ssr_1 = _ols_ssr(X1, y1)
        ssr_2 = _ols_ssr(X2, y2)
        ssr_u = ssr_1 + ssr_2

        df1 = k
        df2 = n - 2 * k
        if df2 < 1 or ssr_u <= 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient df"}

        f_stat = ((ssr_p - ssr_u) / df1) / (ssr_u / df2)
        p_val = float(1.0 - f_dist.cdf(f_stat, df1, df2))

        # Regime estimates for context
        def _regime_stats(X_r: np.ndarray, y_r: np.ndarray) -> dict:
            try:
                beta_r, _, _, _ = np.linalg.lstsq(X_r, y_r, rcond=None)
                return {
                    "intercept": round(float(beta_r[0]), 4),
                    "trend": round(float(beta_r[1]), 4),
                    "n_obs": len(y_r),
                }
            except np.linalg.LinAlgError:
                return {"n_obs": len(y_r)}

        regime1 = _regime_stats(X1, y1)
        regime2 = _regime_stats(X2, y2)
        regime1["period"] = f"{dates[0]}--{dates[mid - 1]}"
        regime2["period"] = f"{dates[mid]}--{dates[-1]}"

        structural_break = p_val < 0.05
        score = float(np.clip(f_stat / 10 * 100, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "breakpoint_date": breakpoint_date,
            "chow_test": {
                "f_statistic": round(float(f_stat), 4),
                "p_value": round(p_val, 4),
                "df": [df1, df2],
                "structural_break": structural_break,
                "ssr_pooled": round(ssr_p, 4),
                "ssr_unrestricted": round(ssr_u, 4),
            },
            "regimes": {
                "period_1": regime1,
                "period_2": regime2,
            },
            "interpretation": (
                "No structural break detected at midpoint (stable GDP growth trend)"
                if not structural_break
                else f"Structural break detected at {breakpoint_date} (F={round(float(f_stat), 2)}, p={round(p_val, 3)})"
            ),
        }
