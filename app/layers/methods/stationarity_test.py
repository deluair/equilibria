"""Stationarity test: ADF unit root test on GDP growth series.

Methodology
-----------
**Augmented Dickey-Fuller (ADF) Test** (Said & Dickey 1984):
    H0: series has a unit root (non-stationary)
    H1: series is stationary

Regression:
    Delta y_t = alpha + beta * y_{t-1} + sum_{j=1}^{p} gamma_j * Delta y_{t-j} + eps_t

ADF t-statistic: t_hat = beta_hat / se(beta_hat)
Critical values (MacKinnon 1994): -3.43 (1%), -2.86 (5%), -2.57 (10%) with constant, no trend.

Lag selection via AIC: minimize AIC = n*ln(SSR/n) + 2k over lags p = 0..p_max.

Score: Non-stationary GDP growth series (fail to reject H0) is a data quality concern,
as growth rates should typically be stationary. Score = 0 (stationary) to 100 (strong
non-stationarity). Mapped from the ADF p-value: score = clip(p_value * 100, 0, 100).

References:
    Said, S.E. & Dickey, D.A. (1984). Testing for unit roots in autoregressive-moving
        average models of unknown order. Biometrika 71(3): 599-607.
    MacKinnon, J.G. (1994). Approximate asymptotic distribution functions for unit-root
        and cointegration tests. Journal of Business & Economic Statistics 12(2): 167-176.
"""

import numpy as np

from app.layers.base import LayerBase

# MacKinnon (1994) approximate critical values for ADF with constant, no trend
_ADF_CV = {"1%": -3.43, "5%": -2.86, "10%": -2.57}


def _adf_aic(y: np.ndarray, p: int) -> tuple[float, float]:
    """Run ADF with p lags. Return (t_stat, aic)."""
    n = len(y)
    dy = np.diff(y)
    T = len(dy) - p
    if T < 5:
        return 0.0, float("inf")
    Y = dy[p:]
    cols = [y[p: p + T], np.ones(T)]
    for j in range(1, p + 1):
        cols.append(dy[p - j: p - j + T])
    X = np.column_stack(cols)
    try:
        beta, _, _, _ = np.linalg.lstsq(X, Y, rcond=None)
    except np.linalg.LinAlgError:
        return 0.0, float("inf")
    resid = Y - X @ beta
    ssr = float(np.sum(resid ** 2))
    k = X.shape[1]
    aic = T * np.log(ssr / T) + 2 * k if ssr > 0 and T > 0 else float("inf")
    s2 = ssr / max(T - k, 1)
    XtX_inv = np.linalg.pinv(X.T @ X)
    se = np.sqrt(s2 * XtX_inv[0, 0]) if s2 > 0 and XtX_inv[0, 0] > 0 else 1e-10
    t_stat = float(beta[0]) / se if se > 0 else 0.0
    return t_stat, aic


class StationarityTest(LayerBase):
    layer_id = "l18"
    name = "Stationarity Test"

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

        values = [float(r["value"]) for r in rows if r["value"] is not None]

        if len(values) < 15:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data for ADF test"}

        y = np.array(values)
        n = len(y)

        # AIC lag selection (max lags = min(10, floor(n/3)))
        p_max = min(10, n // 3)
        best_p, best_aic, best_t = 0, float("inf"), 0.0
        for p in range(0, p_max + 1):
            t, aic = _adf_aic(y, p)
            if aic < best_aic:
                best_aic, best_p, best_t = aic, p, t

        # Approximate p-value via MacKinnon response surface (constant, no trend)
        # tau distribution approximation: use normal as conservative upper bound
        # For practical use: map t-stat to p-value relative to critical values
        t_stat = best_t
        if t_stat <= _ADF_CV["1%"]:
            p_value = 0.01
        elif t_stat <= _ADF_CV["5%"]:
            p_value = 0.05
        elif t_stat <= _ADF_CV["10%"]:
            p_value = 0.10
        else:
            # Non-stationary: interpolate above 0.10
            # Use statsmodels if available for precise p-value
            try:
                from statsmodels.tsa.stattools import adfuller
                result = adfuller(y, maxlag=best_p, autolag=None, regression="c")
                t_stat = float(result[0])
                p_value = float(result[1])
                best_p = int(result[2])
            except Exception:
                # Rough linear interpolation beyond 10% critical value
                excess = t_stat - _ADF_CV["10%"]
                p_value = min(0.10 + excess * 0.05, 1.0)

        stationary = p_value < 0.05
        score = float(np.clip(p_value * 100, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "adf_test": {
                "t_statistic": round(t_stat, 4),
                "p_value": round(p_value, 4),
                "lags_used": best_p,
                "stationary": stationary,
                "critical_values": _ADF_CV,
            },
            "interpretation": (
                "GDP growth series is stationary (no unit root concern)"
                if stationary
                else "GDP growth series appears non-stationary (unit root concern)"
            ),
        }
