"""Specification test: Ramsey RESET test for functional form misspecification.

Methodology
-----------
**Ramsey RESET Test** (Regression Equation Specification Error Test, Ramsey 1969):
    H0: correct functional form (no misspecification)
    H1: functional form is misspecified

Procedure:
1. Estimate base OLS: y_t = alpha + beta * x_t + eps_t
   where y = GDP growth (NY.GDP.MKTP.KD.ZG), x = investment share (NE.GDI.TOTL.ZS)

2. Compute fitted values y_hat_t

3. Augmented regression:
       y_t = alpha + beta * x_t + gamma_2 * y_hat_t^2 + gamma_3 * y_hat_t^3 + u_t

4. F-test: H0: gamma_2 = gamma_3 = 0
       F = [(SSR_R - SSR_U) / q] / [SSR_U / (n - k_U)]
   where q = number of added terms (2: squared + cubed fitted values)

Large F (low p-value) = reject H0, nonlinear terms are significant -> misspecification.
Score = clip(f_stat / f_critical_5pct * 100, 0, 100).

References:
    Ramsey, J.B. (1969). Tests for specification errors in classical linear
        least-squares regression analysis. Journal of the Royal Statistical
        Society B 31(2): 350-371.
"""

import numpy as np
from scipy.stats import f as f_dist

from app.layers.base import LayerBase


class SpecificationTest(LayerBase):
    layer_id = "l18"
    name = "Specification Test"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

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

        rows_inv = await db.fetch_all(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.indicator_code = 'NE.GDI.TOTL.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        gdp_map = {r["date"]: float(r["value"]) for r in rows_gdp if r["value"] is not None}
        inv_map = {r["date"]: float(r["value"]) for r in rows_inv if r["value"] is not None}
        common_dates = sorted(set(gdp_map) & set(inv_map))

        if len(common_dates) < 15:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient matched observations"}

        y = np.array([gdp_map[d] for d in common_dates])
        x = np.array([inv_map[d] for d in common_dates])
        n = len(y)

        # Try statsmodels RESET
        try:
            from statsmodels.stats.diagnostic import linear_reset
            from statsmodels.regression.linear_model import OLS
            import statsmodels.api as sm
            X_sm = sm.add_constant(x)
            model = OLS(y, X_sm).fit()
            # power=3: adds y_hat^2 and y_hat^3 to the regression
            reset_res = linear_reset(model, power=3, use_f=True)
            f_stat = float(reset_res.statistic)
            p_val = float(reset_res.pvalue)
            df1 = 2
            df2 = n - 4
        except Exception:
            # Manual RESET
            X_base = np.column_stack([np.ones(n), x])
            try:
                beta_base, _, _, _ = np.linalg.lstsq(X_base, y, rcond=None)
            except np.linalg.LinAlgError:
                return {"score": None, "signal": "UNAVAILABLE", "error": "base OLS failed"}
            y_hat = X_base @ beta_base
            ssr_r = float(np.sum((y - y_hat) ** 2))

            # Augmented with y_hat^2 and y_hat^3
            X_aug = np.column_stack([np.ones(n), x, y_hat ** 2, y_hat ** 3])
            try:
                beta_aug, _, _, _ = np.linalg.lstsq(X_aug, y, rcond=None)
            except np.linalg.LinAlgError:
                return {"score": None, "signal": "UNAVAILABLE", "error": "augmented OLS failed"}
            y_hat_aug = X_aug @ beta_aug
            ssr_u = float(np.sum((y - y_hat_aug) ** 2))

            df1 = 2  # two added terms
            df2 = n - 4  # n - k_unrestricted
            if df2 < 1 or ssr_u <= 0:
                return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient df"}

            f_stat = ((ssr_r - ssr_u) / df1) / (ssr_u / df2)
            p_val = float(1.0 - f_dist.cdf(f_stat, df1, df2))

        misspecified = p_val < 0.05
        f_critical_5pct = float(f_dist.ppf(0.95, df1, max(df2, 1)))
        score = float(np.clip(f_stat / f_critical_5pct * 100, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "reset_test": {
                "f_statistic": round(f_stat, 4),
                "p_value": round(p_val, 4),
                "df": [df1, max(df2, 1)],
                "f_critical_5pct": round(f_critical_5pct, 4),
                "misspecified": misspecified,
                "added_terms": ["y_hat^2", "y_hat^3"],
            },
            "interpretation": (
                "No functional form misspecification detected (RESET p >= 0.05)"
                if not misspecified
                else f"Functional form misspecification indicated (RESET p={round(p_val, 3)})"
            ),
        }
