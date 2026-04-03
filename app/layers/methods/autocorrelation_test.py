"""Autocorrelation test: Durbin-Watson statistic on GDP growth OLS trend residuals.

Methodology
-----------
**Durbin-Watson (DW) Test** (Durbin & Watson 1950, 1951):
    H0: no first-order serial autocorrelation (rho = 0)
    H1: positive (DW < 2) or negative (DW > 2) autocorrelation

Statistic:
    DW = sum_{t=2}^{T} (e_t - e_{t-1})^2 / sum_{t=1}^{T} e_t^2

Expected value under H0: DW ~= 2 (for large T)
    DW < 2: positive autocorrelation
    DW > 2: negative autocorrelation
    DW ~= 2: no autocorrelation

Interpretation (Savin & White 1977 bounds, n=25, k=1):
    Reject H0 (positive autocorr): DW < 1.29
    Inconclusive: 1.29 <= DW < 1.45
    No autocorr: 1.45 <= DW <= 2.55
    Inconclusive: 2.55 < DW <= 2.71
    Reject H0 (negative autocorr): DW > 2.71

Regression: y_t = alpha + beta * t + eps_t (linear time trend)

Score = clip(abs(dw - 2) * 50, 0, 100). Score 0 = DW exactly 2 (no autocorrelation).

References:
    Durbin, J. & Watson, G.S. (1950). Testing for serial correlation in least
        squares regression I. Biometrika 37(3-4): 409-428.
    Durbin, J. & Watson, G.S. (1951). Testing for serial correlation in least
        squares regression II. Biometrika 38(1-2): 159-178.
"""

import numpy as np

from app.layers.base import LayerBase


class AutocorrelationTest(LayerBase):
    layer_id = "l18"
    name = "Autocorrelation Test"

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
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        y = np.array(values)
        n = len(y)
        t = np.arange(n, dtype=float)

        # OLS: y = alpha + beta * t
        X = np.column_stack([np.ones(n), t])
        try:
            beta, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
        except np.linalg.LinAlgError:
            return {"score": None, "signal": "UNAVAILABLE", "error": "OLS failed"}
        resid = y - X @ beta

        # Durbin-Watson statistic
        diff_resid = np.diff(resid)
        dw = float(np.sum(diff_resid ** 2) / np.sum(resid ** 2)) if np.sum(resid ** 2) > 0 else 2.0

        # First-order autocorrelation coefficient rho_hat = corr(e_t, e_{t-1})
        rho_hat = float(np.corrcoef(resid[1:], resid[:-1])[0, 1]) if n > 2 else 0.0

        # Classification
        if dw < 1.29:
            classification = "positive autocorrelation"
        elif dw < 1.45:
            classification = "inconclusive (positive)"
        elif dw <= 2.55:
            classification = "no autocorrelation"
        elif dw <= 2.71:
            classification = "inconclusive (negative)"
        else:
            classification = "negative autocorrelation"

        problem_detected = dw < 1.45 or dw > 2.55
        score = float(np.clip(abs(dw - 2.0) * 50, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "dw_test": {
                "dw_statistic": round(dw, 4),
                "rho_hat": round(rho_hat, 4),
                "classification": classification,
                "problem_detected": problem_detected,
                "bounds_5pct": {"lower": 1.29, "upper": 2.71},
            },
            "interpretation": (
                "No significant autocorrelation in GDP growth residuals"
                if not problem_detected
                else f"Autocorrelation detected: {classification} (DW={round(dw, 3)})"
            ),
        }
