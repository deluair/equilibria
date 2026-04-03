"""Multicollinearity test: VIF test for collinearity between macro predictors.

Methodology
-----------
**Variance Inflation Factor (VIF)**:
    VIF_j = 1 / (1 - R^2_j)
    where R^2_j is the R-squared from regressing predictor j on all other predictors.

VIF interpretation:
    VIF = 1: no collinearity
    VIF 1-5: low collinearity (acceptable)
    VIF 5-10: moderate collinearity (concern)
    VIF > 10: high collinearity (severe problem)

Rule of thumb: max pairwise |correlation| > 0.9 also indicates multicollinearity.

Predictors: inflation (FP.CPI.TOTL.ZG), unemployment (SL.UEM.TOTL.ZS),
            trade openness (NE.TRD.GNFS.ZS)

High multicollinearity inflates standard errors and makes coefficient estimates
unreliable. Score derived from max pairwise correlation.

Score = clip(max(abs(corr[i,j])) * 100 - 50, 0, 100) * 2
    - Max corr 0.90+: score 80+
    - Max corr 0.75: score 50
    - Max corr < 0.5: score near 0

References:
    Belsley, D.A., Kuh, E. & Welsch, R.E. (1980). Regression Diagnostics:
        Identifying Influential Data and Sources of Collinearity. Wiley.
    O'Brien, R.M. (2007). A caution regarding rules of thumb for variance
        inflation factors. Quality & Quantity 41(5): 673-690.
"""

import numpy as np

from app.layers.base import LayerBase

_INDICATORS = {
    "inflation": "FP.CPI.TOTL.ZG",
    "unemployment": "SL.UEM.TOTL.ZS",
    "trade_openness": "NE.TRD.GNFS.ZS",
}


class MulticollinearityTest(LayerBase):
    layer_id = "l18"
    name = "Multicollinearity Test"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        series_data: dict[str, dict[str, float]] = {}
        for label, code in _INDICATORS.items():
            rows = await db.fetch_all(
                """
                SELECT dp.value, dp.date
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.indicator_code = ?
                ORDER BY dp.date
                """,
                (country, code),
            )
            series_data[label] = {r["date"]: float(r["value"]) for r in rows if r["value"] is not None}

        # Find common dates
        all_dates = set.intersection(*[set(v.keys()) for v in series_data.values()])
        common_dates = sorted(all_dates)

        if len(common_dates) < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient matched observations"}

        labels = list(_INDICATORS.keys())
        X = np.array([[series_data[lbl][d] for lbl in labels] for d in common_dates])
        n, k = X.shape

        # Pairwise correlation matrix
        corr_matrix = np.corrcoef(X.T)

        pairwise = {}
        max_abs_corr = 0.0
        max_pair = None
        for i in range(k):
            for j in range(i + 1, k):
                c = float(corr_matrix[i, j])
                pair_key = f"{labels[i]}_vs_{labels[j]}"
                pairwise[pair_key] = round(c, 4)
                if abs(c) > max_abs_corr:
                    max_abs_corr = abs(c)
                    max_pair = pair_key

        # VIF for each predictor
        vif_results = {}
        for j in range(k):
            y_j = X[:, j]
            X_others = np.delete(X, j, axis=1)
            X_aug = np.column_stack([np.ones(n), X_others])
            try:
                beta_j, _, _, _ = np.linalg.lstsq(X_aug, y_j, rcond=None)
                y_hat_j = X_aug @ beta_j
                ss_res = float(np.sum((y_j - y_hat_j) ** 2))
                ss_tot = float(np.sum((y_j - np.mean(y_j)) ** 2))
                r2_j = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
                vif_j = 1.0 / (1.0 - r2_j) if r2_j < 1.0 else float("inf")
            except np.linalg.LinAlgError:
                vif_j = float("inf")
            vif_results[labels[j]] = round(vif_j, 4)

        max_vif = max(v for v in vif_results.values() if v != float("inf"))
        multicollinear = max_abs_corr > 0.9 or max_vif > 10

        # Score based on max pairwise correlation
        score = float(np.clip((max_abs_corr - 0.5) * 200, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "pairwise_correlations": pairwise,
            "max_correlation": round(max_abs_corr, 4),
            "max_correlation_pair": max_pair,
            "vif": vif_results,
            "max_vif": round(max_vif, 4),
            "multicollinear": multicollinear,
            "interpretation": (
                "No significant multicollinearity detected (max |corr| <= 0.9, max VIF <= 10)"
                if not multicollinear
                else f"Multicollinearity concern: max |corr|={round(max_abs_corr, 3)}, max VIF={round(max_vif, 2)}"
            ),
        }
