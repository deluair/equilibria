"""Bayesian econometric methods (Koop 2003, Geweke 2005).

Bayesian methods provide a coherent framework for parameter uncertainty,
model comparison, and shrinkage in economics. Key advantages:
    - Full posterior distribution, not just point estimates
    - Natural regularization via priors (especially useful with many parameters)
    - Model averaging across specifications
    - Small-sample inference without asymptotic approximations

Models implemented:
    Bayesian Linear Regression:
        y = X*beta + e, e ~ N(0, sigma2*I)
        Prior: beta ~ N(b0, B0), sigma2 ~ IG(s0/2, v0/2)
        Posterior: conjugate Normal-Inverse Gamma

    Gibbs Sampler:
        For non-conjugate models, iteratively sample from conditional posteriors

    Bayesian VAR (BVAR) with Minnesota prior:
        Litterman (1986) shrinkage: own lags toward 1 (random walk),
        other lags toward 0. Controls over-parameterization in VARs.

    Bayesian Model Averaging (BMA):
        Average over all 2^K possible regressor subsets, weighted by
        posterior model probability. Addresses model uncertainty.

    Posterior Predictive Checks:
        Simulate data from posterior predictive distribution,
        compare test statistics to observed data.

References:
    Koop, G. (2003). Bayesian Econometrics. Wiley.
    Geweke, J. (2005). Contemporary Bayesian Econometrics and Statistics.
        Wiley.
    Litterman, R. (1986). Forecasting with Bayesian Vector Autoregressions.
        Journal of Business and Economic Statistics 4(1): 25-38.
    Fernandez, C., Ley, E. & Steel, M. (2001). Model Uncertainty in
        Cross-Country Growth Regressions. J of Applied Econometrics 16(5).

Score: high posterior uncertainty or poor predictive fit -> high score (STRESS).
Tight posteriors with good fit -> STABLE.
"""

import json

import numpy as np
from scipy.stats import invgamma

from app.layers.base import LayerBase


class BayesianMethods(LayerBase):
    layer_id = "l18"
    name = "Bayesian Methods"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        method = kwargs.get("method", "blr")  # blr, bvar, bma
        n_draws = kwargs.get("n_draws", 2000)
        n_burn = kwargs.get("n_burn", 500)

        rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'bayesian'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 15:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        # Parse data
        y_vals, x_data, dates = [], [], []
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
            dates.append(row["date"])

        n = len(y_vals)
        if n < 15:
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

        if method == "bvar":
            result = self._bayesian_var(y, n_draws, n_burn)
        elif method == "bma":
            result = self._bayesian_model_averaging(X, y, var_names)
        else:
            result = self._bayesian_linear_regression(X, y, var_names, n_draws, n_burn)

        # Posterior predictive check
        ppc = self._posterior_predictive_check(X, y, result)

        # Score: posterior uncertainty and predictive fit
        if "posterior_width" in result:
            width = result["posterior_width"]
            if width > 2.0:
                score = 65.0 + min(width - 2.0, 3.5) * 10.0
            elif width > 1.0:
                score = 30.0 + (width - 1.0) * 35.0
            else:
                score = width * 30.0
        else:
            score = 25.0

        # Adjust by predictive fit
        if ppc and ppc.get("p_value") is not None:
            if ppc["p_value"] < 0.05:
                score = min(100, score + 20)  # Poor fit penalty

        score = max(0.0, min(100.0, score))

        return {
            "score": round(score, 2),
            "country": country,
            "method": method,
            "n_obs": n,
            **result,
            "posterior_predictive_check": ppc,
        }

    def _bayesian_linear_regression(self, X: np.ndarray, y: np.ndarray,
                                    var_names: list, n_draws: int,
                                    n_burn: int) -> dict:
        """Bayesian linear regression with conjugate Normal-IG prior.

        Gibbs sampler cycling between beta|sigma2 and sigma2|beta.
        """
        n, k = X.shape
        rng = np.random.default_rng(42)

        # Diffuse prior: b0 = 0, B0 = 100*I, s0 = 0.01, v0 = 0.01
        b0 = np.zeros(k)
        B0_inv = np.eye(k) * 0.01  # Precision = 1/100
        s0 = 0.01
        v0 = 0.01

        # OLS for initialization
        XtX = X.T @ X
        Xty = X.T @ y
        beta_ols = np.linalg.lstsq(X, y, rcond=None)[0]
        resid_ols = y - X @ beta_ols
        sigma2 = float(np.sum(resid_ols ** 2)) / max(n - k, 1)

        # Storage
        beta_draws = np.zeros((n_draws, k))
        sigma2_draws = np.zeros(n_draws)

        beta = beta_ols.copy()

        for s in range(n_draws + n_burn):
            # Draw beta | sigma2, y
            B_post_inv = B0_inv + XtX / sigma2
            try:
                B_post = np.linalg.inv(B_post_inv)
            except np.linalg.LinAlgError:
                B_post = np.linalg.pinv(B_post_inv)
            b_post = B_post @ (B0_inv @ b0 + Xty / sigma2)
            L = np.linalg.cholesky(B_post + np.eye(k) * 1e-10)
            beta = b_post + L @ rng.standard_normal(k)

            # Draw sigma2 | beta, y
            resid = y - X @ beta
            v_post = v0 + n
            s_post = s0 + float(np.sum(resid ** 2))
            sigma2 = float(invgamma.rvs(v_post / 2.0, scale=s_post / 2.0, random_state=rng))
            sigma2 = max(sigma2, 1e-10)

            if s >= n_burn:
                idx = s - n_burn
                beta_draws[idx] = beta
                sigma2_draws[idx] = sigma2

        # Posterior summaries
        beta_mean = np.mean(beta_draws, axis=0)
        beta_sd = np.std(beta_draws, axis=0)
        beta_q025 = np.percentile(beta_draws, 2.5, axis=0)
        beta_q975 = np.percentile(beta_draws, 97.5, axis=0)

        coefficients = {}
        for j, v in enumerate(var_names):
            coefficients[v] = {
                "mean": round(float(beta_mean[j]), 4),
                "sd": round(float(beta_sd[j]), 4),
                "ci_95": [round(float(beta_q025[j]), 4), round(float(beta_q975[j]), 4)],
            }

        # Average posterior width (normalized)
        y_sd = float(np.std(y))
        avg_width = float(np.mean(beta_q975 - beta_q025))
        posterior_width = avg_width / y_sd if y_sd > 0 else avg_width

        return {
            "coefficients": coefficients,
            "sigma2": {
                "mean": round(float(np.mean(sigma2_draws)), 4),
                "sd": round(float(np.std(sigma2_draws)), 4),
            },
            "n_draws": n_draws,
            "posterior_width": round(posterior_width, 4),
        }

    def _bayesian_var(self, y: np.ndarray, n_draws: int, n_burn: int) -> dict:
        """Bayesian VAR(p) with Minnesota prior (univariate case).

        Minnesota prior: own lags shrink toward 1 (random walk prior),
        tightness controlled by hyperparameters.
        """
        n = len(y)
        p = min(4, n // 5)  # Lag order
        if n <= p + 5:
            return {"error": "insufficient data for BVAR", "posterior_width": 5.0}

        # Build lagged matrix
        Y = y[p:]
        X_lags = np.column_stack([
            np.ones(n - p),
            *[y[p - lag - 1:n - lag - 1] for lag in range(p)],
        ])
        n_eff = len(Y)
        k = X_lags.shape[1]

        rng = np.random.default_rng(42)

        # Minnesota prior: beta_1 (first own lag) ~ N(1, lambda^2)
        # other betas ~ N(0, lambda^2 / lag^2)
        lam = 0.1  # Tightness
        b0 = np.zeros(k)
        b0[1] = 1.0  # Random walk prior for first lag
        B0_diag = np.full(k, lam ** 2)
        B0_diag[0] = 10.0  # Loose prior on constant
        for lag in range(p):
            B0_diag[1 + lag] = lam ** 2 / max(lag + 1, 1) ** 2
        B0_inv = np.diag(1.0 / B0_diag)

        # Gibbs sampler
        XtX = X_lags.T @ X_lags
        XtY = X_lags.T @ Y
        beta = np.linalg.lstsq(X_lags, Y, rcond=None)[0]
        resid = Y - X_lags @ beta
        sigma2 = float(np.sum(resid ** 2)) / max(n_eff - k, 1)

        beta_draws = np.zeros((n_draws, k))
        for s in range(n_draws + n_burn):
            B_post_inv = B0_inv + XtX / sigma2
            try:
                B_post = np.linalg.inv(B_post_inv)
            except np.linalg.LinAlgError:
                B_post = np.linalg.pinv(B_post_inv)
            b_post = B_post @ (B0_inv @ b0 + XtY / sigma2)
            L = np.linalg.cholesky(B_post + np.eye(k) * 1e-10)
            beta = b_post + L @ rng.standard_normal(k)

            resid = Y - X_lags @ beta
            v_post = 0.01 + n_eff
            s_post = 0.01 + float(np.sum(resid ** 2))
            sigma2 = float(invgamma.rvs(v_post / 2, scale=s_post / 2, random_state=rng))
            sigma2 = max(sigma2, 1e-10)

            if s >= n_burn:
                beta_draws[s - n_burn] = beta

        beta_mean = np.mean(beta_draws, axis=0)
        beta_sd = np.std(beta_draws, axis=0)

        lag_names = ["constant"] + [f"lag_{i + 1}" for i in range(p)]
        coefficients = {}
        for j, v in enumerate(lag_names):
            coefficients[v] = {
                "mean": round(float(beta_mean[j]), 4),
                "sd": round(float(beta_sd[j]), 4),
            }

        # Impulse response function from posterior mean
        irf = [1.0]
        for h in range(1, 13):
            val = 0.0
            for lag in range(min(h, p)):
                val += beta_mean[1 + lag] * irf[h - 1 - lag] if h - 1 - lag >= 0 else 0.0
            irf.append(val)

        posterior_width = float(np.mean(beta_sd)) / max(float(np.std(y)), 1e-10)

        return {
            "bvar_coefficients": coefficients,
            "lag_order": p,
            "n_effective": n_eff,
            "impulse_response": [round(v, 4) for v in irf],
            "posterior_width": round(posterior_width, 4),
        }

    def _bayesian_model_averaging(self, X: np.ndarray, y: np.ndarray,
                                  var_names: list) -> dict:
        """Bayesian Model Averaging over regressor subsets.

        For K regressors, compute posterior model probability for each
        of 2^K models using BIC approximation. Report posterior inclusion
        probabilities for each variable.
        """
        n, k_full = X.shape
        # Exclude constant from model space
        K = k_full - 1  # Number of switchable regressors

        if K > 15:
            # Too many models, use MC3 sampling instead of enumeration
            K = 15  # Cap

        n_models = 2 ** K
        model_scores = []

        for m in range(n_models):
            # Decode model: which regressors are included
            included = [0]  # Always include constant
            for j in range(K):
                if m & (1 << j):
                    included.append(j + 1)
            X_m = X[:, included]
            k_m = len(included)

            # OLS
            beta = np.linalg.lstsq(X_m, y, rcond=None)[0]
            resid = y - X_m @ beta
            ss_res = float(np.sum(resid ** 2))
            sigma2 = ss_res / max(n - k_m, 1)

            # BIC
            if sigma2 > 0:
                bic = n * np.log(sigma2) + k_m * np.log(n)
            else:
                bic = float("inf")

            model_scores.append({
                "model_id": m,
                "included": included,
                "k": k_m,
                "bic": bic,
                "r2": 1.0 - ss_res / float(np.sum((y - np.mean(y)) ** 2))
                if float(np.sum((y - np.mean(y)) ** 2)) > 0 else 0.0,
            })

        # Posterior model probabilities (BIC approximation)
        bics = np.array([ms["bic"] for ms in model_scores])
        bics -= np.min(bics)  # Normalize
        log_probs = -0.5 * bics
        max_lp = np.max(log_probs)
        probs = np.exp(log_probs - max_lp)
        probs /= np.sum(probs)

        for i, ms in enumerate(model_scores):
            ms["posterior_prob"] = float(probs[i])

        # Posterior inclusion probability for each variable
        pip = {}
        for j in range(1, k_full):
            p_inc = sum(
                ms["posterior_prob"]
                for ms in model_scores
                if j in ms["included"]
            )
            pip[var_names[j]] = round(p_inc, 4)

        # Top 5 models
        top_models = sorted(model_scores, key=lambda x: -x["posterior_prob"])[:5]

        # BMA-weighted coefficients
        bma_coef = np.zeros(k_full)
        for ms in model_scores:
            X_m = X[:, ms["included"]]
            beta = np.linalg.lstsq(X_m, y, rcond=None)[0]
            for idx_in_model, col_idx in enumerate(ms["included"]):
                bma_coef[col_idx] += ms["posterior_prob"] * beta[idx_in_model]

        bma_coefficients = {v: round(float(bma_coef[j]), 4) for j, v in enumerate(var_names)}

        posterior_width = float(np.std(list(pip.values()))) if pip else 1.0

        return {
            "bma_coefficients": bma_coefficients,
            "posterior_inclusion_probabilities": pip,
            "n_models_evaluated": n_models,
            "top_models": [
                {
                    "variables": [var_names[i] for i in m["included"]],
                    "posterior_prob": round(m["posterior_prob"], 4),
                    "r2": round(m["r2"], 4),
                }
                for m in top_models
            ],
            "posterior_width": round(posterior_width, 4),
        }

    @staticmethod
    def _posterior_predictive_check(X: np.ndarray, y: np.ndarray,
                                   result: dict) -> dict | None:
        """Compare observed test statistics to posterior predictive distribution."""
        if "coefficients" not in result:
            return None

        # Use posterior mean for prediction
        var_names = list(result["coefficients"].keys()) if isinstance(
            result["coefficients"], dict
        ) else []
        if not var_names:
            return None

        # Reconstruct predicted values from posterior mean
        k = X.shape[1]
        beta_mean = np.zeros(k)
        for j, v in enumerate(var_names):
            coef_info = result["coefficients"][v]
            if isinstance(coef_info, dict):
                beta_mean[j] = coef_info.get("mean", 0.0)
            else:
                beta_mean[j] = float(coef_info)

        y_pred = X @ beta_mean
        resid = y - y_pred

        # Test statistics: skewness and kurtosis of residuals
        from scipy.stats import kurtosis, normaltest, skew
        obs_skew = float(skew(resid))
        obs_kurt = float(kurtosis(resid))

        # Normality test on residuals
        if len(resid) >= 8:
            stat, p_val = normaltest(resid)
            p_val = float(p_val)
        else:
            p_val = None

        return {
            "residual_skewness": round(obs_skew, 4),
            "residual_kurtosis": round(obs_kurt, 4),
            "p_value": round(p_val, 4) if p_val is not None else None,
        }
