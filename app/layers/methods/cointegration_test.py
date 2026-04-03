"""Cointegration test: Engle-Granger test for trade openness and GDP levels.

Methodology
-----------
**Engle-Granger Two-Step Cointegration Test** (Engle & Granger 1987):
Step 1: Estimate OLS regression of y on x (levels):
    y_t = alpha + beta * x_t + eps_t

Step 2: Test residuals eps_t for stationarity using ADF.
    H0: residuals have a unit root (no cointegration)
    H1: residuals are stationary (cointegrated)

Critical values for Engle-Granger (MacKinnon 1990/2010, n=2 variables):
    1%: -3.90, 5%: -3.34, 10%: -3.04

Trade openness (NE.TRD.GNFS.ZS) and GDP levels (NY.GDP.MKTP.KD) are expected to
be cointegrated if the trade-GDP relationship is stable in the long run.
No cointegration where expected = model specification concern.

Score = 0 (cointegrated, no concern) to 100 (no cointegration, severe concern).

References:
    Engle, R.F. & Granger, C.W.J. (1987). Co-integration and error correction:
        Representation, estimation, and testing. Econometrica 55(2): 251-276.
    MacKinnon, J.G. (2010). Critical values for cointegration tests.
        Queen's Economics Department Working Paper No. 1227.
"""

import numpy as np

from app.layers.base import LayerBase

# MacKinnon (2010) critical values for EG test, n=2 variables, with constant
_EG_CV = {"1%": -3.90, "5%": -3.34, "10%": -3.04}


def _adf_stat_resid(e: np.ndarray, lags: int = 1) -> float:
    """ADF t-stat on OLS residuals (no constant, no trend)."""
    n = len(e)
    dy = np.diff(e)
    p = min(lags, len(dy) - 2)
    if p < 0:
        p = 0
    T = len(dy) - p
    if T < 5:
        return 0.0
    Y = dy[p:]
    cols = [e[p: p + T]]
    for j in range(1, p + 1):
        cols.append(dy[p - j: p - j + T])
    X = np.column_stack(cols) if len(cols) > 1 else cols[0].reshape(-1, 1)
    try:
        beta, _, _, _ = np.linalg.lstsq(X, Y, rcond=None)
    except np.linalg.LinAlgError:
        return 0.0
    resid = Y - X @ beta
    ssr = float(np.sum(resid ** 2))
    k = X.shape[1]
    s2 = ssr / max(T - k, 1)
    XtX_inv = np.linalg.pinv(X.T @ X)
    se = np.sqrt(s2 * XtX_inv[0, 0]) if s2 > 0 and XtX_inv[0, 0] > 0 else 1e-10
    return float(beta[0]) / se if se > 0 else 0.0


class CointegrationTest(LayerBase):
    layer_id = "l18"
    name = "Cointegration Test"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

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
              AND ds.indicator_code = 'NY.GDP.MKTP.KD'
            ORDER BY dp.date
            """,
            (country,),
        )

        trade_map = {r["date"]: float(r["value"]) for r in rows_trade if r["value"] is not None}
        gdp_map = {r["date"]: float(r["value"]) for r in rows_gdp if r["value"] is not None}
        common_dates = sorted(set(trade_map) & set(gdp_map))

        if len(common_dates) < 15:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient matched observations"}

        x = np.array([trade_map[d] for d in common_dates])
        y = np.log(np.array([gdp_map[d] for d in common_dates]) + 1e-10)

        # Step 1: OLS regression
        X = np.column_stack([np.ones(len(x)), x])
        try:
            beta, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
        except np.linalg.LinAlgError:
            return {"score": None, "signal": "UNAVAILABLE", "error": "OLS failed"}
        resid = y - X @ beta

        # Step 2: ADF on residuals
        # Try statsmodels first for precise critical values
        eg_t = None
        p_value = None
        lags_used = 1
        try:
            from statsmodels.tsa.stattools import adfuller
            result = adfuller(resid, maxlag=4, autolag="AIC", regression="nc")
            eg_t = float(result[0])
            p_value = float(result[1])
            lags_used = int(result[2])
        except Exception:
            eg_t = _adf_stat_resid(resid, lags=1)
            # Map to p-value via EG critical values
            if eg_t <= _EG_CV["1%"]:
                p_value = 0.01
            elif eg_t <= _EG_CV["5%"]:
                p_value = 0.05
            elif eg_t <= _EG_CV["10%"]:
                p_value = 0.10
            else:
                excess = eg_t - _EG_CV["10%"]
                p_value = min(0.10 + abs(excess) * 0.05, 1.0)

        cointegrated = eg_t is not None and eg_t <= _EG_CV["5%"]
        score = float(np.clip((1.0 - (1.0 if cointegrated else 0.0)) * 70 + (p_value or 0.5) * 30, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": len(common_dates),
            "ols_regression": {
                "intercept": round(float(beta[0]), 4),
                "slope_trade_openness": round(float(beta[1]), 4),
            },
            "eg_test": {
                "t_statistic": round(eg_t, 4) if eg_t is not None else None,
                "p_value": round(p_value, 4) if p_value is not None else None,
                "lags": lags_used,
                "cointegrated": cointegrated,
                "critical_values": _EG_CV,
            },
            "interpretation": (
                "Trade openness and GDP levels are cointegrated (long-run relationship stable)"
                if cointegrated
                else "No cointegration detected (long-run relationship unstable, specification concern)"
            ),
        }
