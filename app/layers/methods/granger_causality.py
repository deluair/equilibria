"""Granger causality: does trade predict GDP growth?

Methodology
-----------
**Granger Causality Test** (Granger 1969):
    H0: x does NOT Granger-cause y (coefficients on lagged x are jointly zero)
    H1: x Granger-causes y

Restricted model:
    y_t = alpha + sum_{j=1}^{p} beta_j * y_{t-j} + eps_t

Unrestricted model:
    y_t = alpha + sum_{j=1}^{p} beta_j * y_{t-j} + sum_{j=1}^{p} gamma_j * x_{t-j} + eps_t

F-statistic:
    F = [(SSR_R - SSR_U) / p] / [SSR_U / (T - 2p - 1)]
    Under H0: F ~ F(p, T - 2p - 1)

Test with lags p = 1, 2, 3. Report results for each lag length.

Weak causality (fail to reject H0 that trade does not predict growth) = specification
concern for structural models that assume trade-growth linkage.

Score = 0 (strong causality, no concern) to 100 (no causality, maximum concern).
Derived from the minimum p-value across lag lengths.

References:
    Granger, C.W.J. (1969). Investigating causal relations by econometric models
        and cross-spectral methods. Econometrica 37(3): 424-438.
"""

import numpy as np
from scipy.stats import f as f_dist

from app.layers.base import LayerBase


def _build_lag_matrix(arr: np.ndarray, p: int, T: int, offset: int) -> np.ndarray:
    """Build T x p matrix of lagged values starting at offset."""
    cols = []
    for j in range(1, p + 1):
        cols.append(arr[offset - j: offset - j + T])
    return np.column_stack(cols)


def _granger_f(y: np.ndarray, x: np.ndarray, p: int) -> tuple[float, float, int]:
    """Return (F-stat, p-value, df2) for Granger test at lag p."""
    n = len(y)
    T = n - p
    if T < 10:
        return 0.0, 1.0, 0
    y_dep = y[p:]
    y_lags = _build_lag_matrix(y, p, T, p)
    x_lags = _build_lag_matrix(x, p, T, p)

    # Restricted model
    X_r = np.column_stack([np.ones(T), y_lags])
    try:
        beta_r, _, _, _ = np.linalg.lstsq(X_r, y_dep, rcond=None)
        ssr_r = float(np.sum((y_dep - X_r @ beta_r) ** 2))
    except np.linalg.LinAlgError:
        return 0.0, 1.0, 0

    # Unrestricted model
    X_u = np.column_stack([np.ones(T), y_lags, x_lags])
    try:
        beta_u, _, _, _ = np.linalg.lstsq(X_u, y_dep, rcond=None)
        ssr_u = float(np.sum((y_dep - X_u @ beta_u) ** 2))
    except np.linalg.LinAlgError:
        return 0.0, 1.0, 0

    df1 = p
    df2 = T - 2 * p - 1
    if df2 < 1 or ssr_u <= 0:
        return 0.0, 1.0, df2

    f_stat = ((ssr_r - ssr_u) / df1) / (ssr_u / df2)
    p_val = float(1.0 - f_dist.cdf(f_stat, df1, df2))
    return float(f_stat), p_val, df2


class GrangerCausality(LayerBase):
    layer_id = "l18"
    name = "Granger Causality"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        max_lags = kwargs.get("max_lags", 3)

        rows_trade = await db.fetch_all(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.indicator_code = 'NE.TRD.GNFS.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        rows_gdp = await db.fetch_all(
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

        trade_map = {r["date"]: float(r["value"]) for r in rows_trade if r["value"] is not None}
        gdp_map = {r["date"]: float(r["value"]) for r in rows_gdp if r["value"] is not None}
        common_dates = sorted(set(trade_map) & set(gdp_map))

        if len(common_dates) < 15:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient matched observations"}

        y = np.array([gdp_map[d] for d in common_dates])
        x = np.array([trade_map[d] for d in common_dates])

        # Try statsmodels for precision
        sm_results = None
        try:
            from statsmodels.tsa.stattools import grangercausalitytests
            data = np.column_stack([y, x])
            test_res = grangercausalitytests(data, maxlag=max_lags, verbose=False)
            sm_results = {}
            for lag, res in test_res.items():
                # res is (dict_of_tests, list_of_reg_results)
                # Use F-test
                f_stat = float(res[0]["ssr_ftest"][0])
                p_val = float(res[0]["ssr_ftest"][1])
                sm_results[lag] = {"f_statistic": round(f_stat, 4), "p_value": round(p_val, 4)}
        except Exception:
            sm_results = None

        # Fallback: manual F-test
        lag_results = {}
        if sm_results is not None:
            lag_results = sm_results
        else:
            for p in range(1, max_lags + 1):
                f_stat, p_val, df2 = _granger_f(y, x, p)
                lag_results[p] = {
                    "f_statistic": round(f_stat, 4),
                    "p_value": round(p_val, 4),
                    "df2": df2,
                }

        # Min p-value across lags
        min_pval = min(v["p_value"] for v in lag_results.values()) if lag_results else 1.0
        causality_detected = min_pval < 0.10

        # Score: no causality = higher concern
        # p=0 -> score=100, p=0.10 -> score=0 (linear interpolation)
        score = float(np.clip((1.0 - causality_detected) * 60 + min_pval * 40, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": len(common_dates),
            "granger_tests": lag_results,
            "min_p_value": round(min_pval, 4),
            "causality_detected": causality_detected,
            "direction": "trade -> GDP growth",
            "interpretation": (
                "Trade Granger-causes GDP growth (supports trade-growth model)"
                if causality_detected
                else "No Granger causality from trade to growth (structural model concern)"
            ),
        }
