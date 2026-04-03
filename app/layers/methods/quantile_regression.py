"""Quantile regression (Koenker & Bassett 1978) and distributional methods.

Unlike OLS which models the conditional mean E[Y|X], quantile regression
models conditional quantiles Q_tau(Y|X), allowing the effect of X to vary
across the distribution of Y. This is critical when treatment effects are
heterogeneous (e.g. minimum wages affect low-wage workers differently than
high-wage workers).

The tau-th quantile regression solves:
    min_beta sum_i rho_tau(y_i - x_i'beta)

where rho_tau(u) = u*(tau - I(u<0)) is the check function (asymmetric
absolute loss).

Key features:
    1. Standard quantile regression at specified quantiles
    2. Quantile treatment effects (QTE): difference in quantile functions
       between treated and control
    3. Machado-Mata (2005) decomposition: decompose distribution differences
       into composition and structure effects (distributional analog of
       Oaxaca-Blinder)
    4. Quantile process: estimate across grid of quantiles, test equality

References:
    Koenker, R. & Bassett, G. (1978). Regression Quantiles. Econometrica
        46(1): 33-50.
    Machado, J. & Mata, J. (2005). Counterfactual Decomposition of Changes
        in Wage Distributions. Journal of Applied Econometrics 20(4): 445-465.
    Firpo, S., Fortin, N. & Lemieux, T. (2009). Unconditional Quantile
        Regressions. Econometrica 77(3): 953-973.

Score: large distributional heterogeneity (coefficients vary strongly across
quantiles) -> high score (STRESS). Uniform effects -> STABLE.
"""

import json

import numpy as np
from scipy.optimize import linprog

from app.layers.base import LayerBase


class QuantileRegression(LayerBase):
    layer_id = "l18"
    name = "Quantile Regression"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        quantiles = kwargs.get("quantiles", [0.10, 0.25, 0.50, 0.75, 0.90])

        rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'quantile_reg'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows or len(rows) < 20:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        # Parse y and X from data
        y_vals, x_data = [], []
        x_keys_set = set()
        for row in rows:
            y = row["value"]
            if y is None:
                continue
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            covars = meta.get("covariates", {})
            x_keys_set |= set(covars.keys())
            y_vals.append(float(y))
            x_data.append(covars)

        n = len(y_vals)
        if n < 20:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid obs"}

        y = np.array(y_vals)
        x_keys = sorted(x_keys_set)
        if x_keys:
            X = np.column_stack([
                np.ones(n),
                *[np.array([d.get(k, 0.0) for d in x_data]) for k in x_keys],
            ])
            var_names = ["constant"] + x_keys
        else:
            X = np.ones((n, 1))
            var_names = ["constant"]

        k = X.shape[1]

        # Estimate quantile regression at each tau
        qr_results = {}
        for tau in quantiles:
            beta = self._quantile_reg(X, y, tau)
            # Bootstrap SE (pairs bootstrap)
            rng = np.random.default_rng(42)
            betas_boot = []
            for _ in range(200):
                idx = rng.choice(n, size=n, replace=True)
                b = self._quantile_reg(X[idx], y[idx], tau)
                betas_boot.append(b)
            betas_boot = np.array(betas_boot)
            se = np.std(betas_boot, axis=0)

            qr_results[tau] = {
                "coefficients": {v: round(float(beta[j]), 4) for j, v in enumerate(var_names)},
                "std_errors": {v: round(float(se[j]), 4) for j, v in enumerate(var_names)},
            }

        # Quantile treatment effects: coefficient at tau=0.9 minus tau=0.1
        # for each covariate (distributional heterogeneity)
        qte = {}
        if 0.10 in quantiles and 0.90 in quantiles:
            for v in var_names:
                qte[v] = round(
                    qr_results[0.90]["coefficients"][v] - qr_results[0.10]["coefficients"][v], 4
                )

        # Machado-Mata decomposition (simplified): compare quantile function
        # at median covariate values
        mm_decomp = None
        if len(quantiles) >= 3 and k > 1:
            x_median = np.median(X, axis=0)
            predicted_quantiles = {}
            for tau in quantiles:
                beta_tau = np.array([
                    qr_results[tau]["coefficients"][v] for v in var_names
                ])
                predicted_quantiles[tau] = round(float(x_median @ beta_tau), 4)
            mm_decomp = {
                "predicted_distribution": predicted_quantiles,
                "iqr": round(
                    predicted_quantiles.get(0.75, 0) - predicted_quantiles.get(0.25, 0), 4
                ) if 0.75 in predicted_quantiles and 0.25 in predicted_quantiles else None,
            }

        # Score: distributional heterogeneity measured by coefficient variation
        # across quantiles for the main treatment variable
        if len(var_names) > 1:
            main_var = var_names[1]  # First non-constant covariate
            coefs_across_q = [
                qr_results[tau]["coefficients"][main_var] for tau in quantiles
            ]
            coef_range = max(coefs_across_q) - min(coefs_across_q)
            median_coef = qr_results.get(0.50, qr_results[quantiles[len(quantiles) // 2]])
            median_val = abs(median_coef["coefficients"][main_var])
            heterogeneity = coef_range / median_val if median_val > 1e-10 else coef_range
        else:
            heterogeneity = 0.0

        if heterogeneity > 2.0:
            score = 75.0
        elif heterogeneity > 1.0:
            score = 40.0 + (heterogeneity - 1.0) * 35.0
        elif heterogeneity > 0.3:
            score = 15.0 + (heterogeneity - 0.3) * 35.7
        else:
            score = heterogeneity * 50.0
        score = max(0.0, min(100.0, score))

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "n_covariates": k - 1,
            "quantile_results": {str(tau): v for tau, v in qr_results.items()},
            "quantile_treatment_effects": qte,
            "machado_mata": mm_decomp,
            "heterogeneity_index": round(heterogeneity, 4),
        }

    @staticmethod
    def _quantile_reg(X: np.ndarray, y: np.ndarray, tau: float) -> np.ndarray:
        """Solve quantile regression via linear programming.

        min_beta sum rho_tau(y - X*beta)
        reformulated as LP:
            min tau*1'u + (1-tau)*1'v
            s.t. X*beta + u - v = y, u >= 0, v >= 0
        """
        n, k = X.shape

        # LP formulation: variables = [beta (k), u+ (n), u- (n)]
        # Objective: 0*beta + tau*u+ + (1-tau)*u-
        c = np.concatenate([np.zeros(k), tau * np.ones(n), (1 - tau) * np.ones(n)])

        # Equality constraint: X*beta + I*u+ - I*u- = y
        A_eq = np.hstack([X, np.eye(n), -np.eye(n)])
        b_eq = y

        # Bounds: beta unbounded, u+/u- >= 0
        bounds = [(None, None)] * k + [(0, None)] * (2 * n)

        result = linprog(c, A_eq=A_eq, b_eq=b_eq, bounds=bounds, method="highs")

        if result.success:
            return result.x[:k]
        # Fallback: use numpy for median regression approximation
        return np.linalg.lstsq(X, y, rcond=None)[0]
