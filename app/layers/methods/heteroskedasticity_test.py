"""Heteroskedasticity test: White's test on inflation-growth OLS residuals.

Methodology
-----------
**White's Test for Heteroskedasticity** (White 1980):
    H0: homoskedastic errors (constant variance)
    H1: heteroskedastic errors

Procedure:
1. Estimate OLS: growth_t = alpha + beta * inflation_t + eps_t
2. Compute squared residuals e_t^2
3. Regress e_t^2 on regressors, their squares, and cross-products:
       e_t^2 = delta_0 + delta_1 * inflation_t + delta_2 * inflation_t^2 + eta_t
4. Test statistic: LM = n * R^2 from auxiliary regression
   Under H0: LM ~ chi-sq(k) where k = number of regressors in auxiliary regression (excl. const.)

White's test is a general misspecification test: detects any form of heteroskedasticity
without assuming a specific alternative.

High LM statistic (low p-value) indicates heteroskedastic errors, which invalidates
OLS standard errors (inference unreliable without robust SEs).

Score = clip(lm_stat / chi2_95th * 100, 0, 100) where chi2_95th is the 5% critical value.

References:
    White, H. (1980). A Heteroskedasticity-Consistent Covariance Matrix Estimator
        and a Direct Test for Heteroskedasticity. Econometrica 48(4): 817-838.
"""

import numpy as np
from scipy.stats import chi2

from app.layers.base import LayerBase


class HeteroskedasticityTest(LayerBase):
    layer_id = "l18"
    name = "Heteroskedasticity Test"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows_inf = await db.fetch_all(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.indicator_code = 'FP.CPI.TOTL.ZG'
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

        inf_map = {r["date"]: float(r["value"]) for r in rows_inf if r["value"] is not None}
        gdp_map = {r["date"]: float(r["value"]) for r in rows_gdp if r["value"] is not None}
        common_dates = sorted(set(inf_map) & set(gdp_map))

        if len(common_dates) < 15:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient matched observations"}

        x_raw = np.array([inf_map[d] for d in common_dates])
        y = np.array([gdp_map[d] for d in common_dates])
        n = len(y)

        # Try statsmodels first
        try:
            from statsmodels.stats.diagnostic import het_white
            from statsmodels.regression.linear_model import OLS
            import statsmodels.api as sm
            X_sm = sm.add_constant(x_raw)
            model = OLS(y, X_sm).fit()
            lm_stat, lm_pval, f_stat, f_pval = het_white(model.resid, X_sm)
            lm_stat = float(lm_stat)
            lm_pval = float(lm_pval)
            k_aux = 2  # inflation + inflation^2
        except Exception:
            # Manual White's test
            X = np.column_stack([np.ones(n), x_raw])
            try:
                beta, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
            except np.linalg.LinAlgError:
                return {"score": None, "signal": "UNAVAILABLE", "error": "OLS failed"}
            resid = y - X @ beta
            e2 = resid ** 2

            # Auxiliary regression: e^2 on [1, x, x^2]
            X_aux = np.column_stack([np.ones(n), x_raw, x_raw ** 2])
            try:
                beta_aux, _, _, _ = np.linalg.lstsq(X_aux, e2, rcond=None)
            except np.linalg.LinAlgError:
                return {"score": None, "signal": "UNAVAILABLE", "error": "auxiliary regression failed"}
            e2_hat = X_aux @ beta_aux
            ss_res = float(np.sum((e2 - e2_hat) ** 2))
            ss_tot = float(np.sum((e2 - np.mean(e2)) ** 2))
            r2_aux = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

            k_aux = 2  # inflation + inflation^2 (excl. constant)
            lm_stat = n * r2_aux
            lm_pval = float(1.0 - chi2.cdf(lm_stat, df=k_aux))

        heteroskedastic = lm_pval < 0.05
        cv_95 = float(chi2.ppf(0.95, df=k_aux))
        score = float(np.clip(lm_stat / cv_95 * 100, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "white_test": {
                "lm_statistic": round(lm_stat, 4),
                "p_value": round(lm_pval, 4),
                "df": k_aux,
                "critical_value_5pct": round(cv_95, 4),
                "heteroskedastic": heteroskedastic,
            },
            "interpretation": (
                "Homoskedastic errors (OLS standard errors valid)"
                if not heteroskedastic
                else "Heteroskedastic errors detected (use robust standard errors)"
            ),
        }
