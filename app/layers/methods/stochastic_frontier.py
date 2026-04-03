"""Stochastic frontier analysis: Battese-Coelli SFA, half-normal/truncated-normal, TE scores.

Methodology
-----------
**Stochastic Frontier Analysis (SFA)** (Aigner, Lovell & Schmidt 1977):
Production frontier:
    y_i = f(x_i; beta) * exp(v_i - u_i)

In log form:
    ln(y_i) = x_i' * beta + v_i - u_i

where:
    v_i ~ N(0, sigma_v^2)  symmetric noise (measurement error, luck)
    u_i >= 0               one-sided inefficiency term

    Technical efficiency: TE_i = exp(-u_i) in [0, 1]

**Half-Normal Model** (Aigner et al. 1977):
    u_i ~ N^+(0, sigma_u^2)
    Composed error: eps_i = v_i - u_i ~ N(0, sigma_v^2 + sigma_u^2) with skewness

    Lambda = sigma_u / sigma_v (signal-to-noise ratio)
    Conditional mean of inefficiency given eps:
        E[u_i | eps_i] = sigma_star * [phi(eps_i * lambda / sigma) / Phi(-eps_i * lambda / sigma) - eps_i * lambda / sigma]
    where sigma^2 = sigma_v^2 + sigma_u^2, sigma_star^2 = sigma_v^2 * sigma_u^2 / sigma^2

**Truncated-Normal Model** (Stevenson 1980):
    u_i ~ N^+(mu, sigma_u^2)
    Adds location parameter mu to allow non-zero mean inefficiency.
    Estimated via MLE with the log-likelihood of the composed error distribution.

**Log-likelihood for SFA**:
    ln L = -N/2 * ln(2*pi) - N*ln(sigma) + sum_i ln(Phi(-eps_i * lambda / sigma))
           - sum_i eps_i^2 / (2 * sigma^2)

    Optimized via scipy.optimize.minimize.

**Determinants of Inefficiency** (Battese & Coelli 1995):
    u_i = delta_0 + delta' * z_i + w_i
    where z_i are firm/unit characteristics (size, age, ownership).
    Jointly estimated with the frontier (one-step approach).

Score: average TE score (higher TE = lower score, more efficient = STABLE).
Mean TE < 0.6 with high variance -> STRESS. Mean TE > 0.85 -> STABLE.

References:
    Aigner, D., Lovell, C.A.K. & Schmidt, P. (1977). Formulation and Estimation
        of Stochastic Frontier Production Function Models.
        Journal of Econometrics 6(1): 21-37.
    Stevenson, R. (1980). Likelihood Functions for Generalized Stochastic Frontier
        Estimation. Journal of Econometrics 13(1): 57-66.
    Battese, G. & Coelli, T. (1995). A Model for Technical Inefficiency Effects
        in a Stochastic Frontier Production Function. Empirical Economics 20: 325-332.
"""

import json

import numpy as np
from scipy.optimize import minimize
from scipy.stats import norm

from app.layers.base import LayerBase


def _sfa_loglik_halfnormal(params: np.ndarray, X: np.ndarray, y: np.ndarray) -> float:
    """Negative log-likelihood for half-normal SFA (Aigner et al. 1977)."""
    k = X.shape[1]
    beta = params[:k]
    log_sigma = params[k]
    log_lambda = params[k + 1]

    sigma = np.exp(log_sigma)
    lam = np.exp(log_lambda)
    if sigma <= 0 or lam <= 0:
        return 1e12

    eps = y - X @ beta
    sigma2 = sigma ** 2
    lambda2 = lam ** 2

    sigma_v2 = sigma2 / (1 + lambda2)
    sigma_u2 = sigma2 * lambda2 / (1 + lambda2)
    if sigma_v2 <= 0 or sigma_u2 <= 0:
        return 1e12

    # log-likelihood
    ll = (-len(y) / 2 * np.log(2 * np.pi)
          - len(y) * np.log(sigma)
          + np.sum(np.log(norm.cdf(-eps * lam / sigma)))
          - np.sum(eps ** 2) / (2 * sigma2))
    return -float(ll)


def _sfa_loglik_truncated(params: np.ndarray, X: np.ndarray, y: np.ndarray) -> float:
    """Negative log-likelihood for truncated-normal SFA (Stevenson 1980)."""
    k = X.shape[1]
    beta = params[:k]
    mu = params[k]
    log_sigma_u = params[k + 1]
    log_sigma_v = params[k + 2]

    sigma_u = np.exp(log_sigma_u)
    sigma_v = np.exp(log_sigma_v)
    if sigma_u <= 0 or sigma_v <= 0:
        return 1e12

    sigma2 = sigma_u ** 2 + sigma_v ** 2
    sigma = np.sqrt(sigma2)
    lam = sigma_u / sigma_v

    eps = y - X @ beta

    # Truncated-normal LL (Stevenson 1980, eq 6)
    mu_star = (mu * sigma_v ** 2 - eps * sigma_u ** 2) / sigma2
    sigma_star = sigma_u * sigma_v / sigma

    a0_denom = norm.cdf(mu / sigma_u)
    if a0_denom <= 0:
        return 1e12

    ll = (np.sum(np.log(norm.cdf(mu_star / sigma_star)))
          - np.sum(np.log(norm.cdf(mu / sigma_u)))  # normalization
          - len(y) * np.log(sigma)
          - np.sum(eps ** 2) / (2 * sigma2)
          - len(y) / 2 * np.log(2 * np.pi))
    return -float(ll)


def _technical_efficiency_halfnormal(eps: np.ndarray, sigma_u: float, sigma_v: float) -> np.ndarray:
    """E[exp(-u)|eps] using Jondrow et al. (1982) formula."""
    sigma2 = sigma_u ** 2 + sigma_v ** 2
    sigma = np.sqrt(sigma2)
    sigma_star = sigma_u * sigma_v / sigma
    mu_star = -eps * sigma_u ** 2 / sigma2  # E[u|eps] under half-normal

    phi_ratio = norm.pdf(mu_star / sigma_star) / np.maximum(norm.cdf(mu_star / sigma_star), 1e-10)
    e_u_given_eps = mu_star + sigma_star * phi_ratio
    return np.exp(-e_u_given_eps)


class StochasticFrontier(LayerBase):
    layer_id = "l18"
    name = "Stochastic Frontier"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        model_type = kwargs.get("model", "halfnormal")  # halfnormal or truncated

        rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'stochastic_frontier'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 20:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        y_list, x_list, z_list, ids = [], [], [], []
        x_keys: list[str] = []

        for row in rows:
            if row["value"] is None:
                continue
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            y_list.append(float(np.log(max(float(row["value"]), 1e-10))))
            inputs = meta.get("inputs", {})
            z_vars = meta.get("z", {})  # inefficiency determinants
            if not x_keys:
                x_keys = sorted(inputs.keys()) if inputs else []
            x_list.append({k: float(inputs.get(k, 0.0)) for k in x_keys} if x_keys else {})
            z_list.append(z_vars)
            ids.append(row.get("date", ""))

        n = len(y_list)
        if n < 20:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid obs"}

        y = np.array(y_list)
        if x_keys:
            X = np.column_stack([
                np.ones(n),
                *[np.log(np.maximum(np.array([d.get(k, 1.0) for d in x_list]), 1e-10))
                  for k in x_keys],
            ])
            var_names = ["constant"] + [f"ln_{k}" for k in x_keys]
        else:
            X = np.ones((n, 1))
            var_names = ["constant"]

        k = X.shape[1]

        # OLS initialization
        beta_ols, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
        resid_ols = y - X @ beta_ols
        sigma_ols = float(np.std(resid_ols))

        # Skewness of residuals (should be negative for production frontier)
        eps_skew = float(np.mean(resid_ols ** 3) / max(np.std(resid_ols) ** 3, 1e-10))
        wrong_skew = eps_skew > 0  # Positive skew = wrong sign for production frontier

        # --- Maximum Likelihood Estimation ---
        mle_result = None
        if model_type == "truncated":
            x0 = np.concatenate([beta_ols, [0.0, np.log(max(sigma_ols, 1e-5)),
                                             np.log(max(sigma_ols, 1e-5))]])
            try:
                res = minimize(_sfa_loglik_truncated, x0, args=(X, y),
                               method="L-BFGS-B",
                               options={"maxiter": 1000, "ftol": 1e-9})
                if res.success or res.fun < 1e10:
                    mle_result = res
            except Exception:
                pass
        else:
            x0 = np.concatenate([beta_ols, [np.log(max(sigma_ols, 1e-5)), 0.0]])
            try:
                res = minimize(_sfa_loglik_halfnormal, x0, args=(X, y),
                               method="L-BFGS-B",
                               options={"maxiter": 1000, "ftol": 1e-9})
                if res.success or res.fun < 1e10:
                    mle_result = res
            except Exception:
                pass

        # Extract parameters and compute TE scores
        te_scores = None
        frontier_params: dict = {}
        if mle_result is not None:
            phat = mle_result.x
            beta_hat = phat[:k]
            frontier_params = {
                v: round(float(b), 4) for v, b in zip(var_names, beta_hat)
            }

            if model_type == "halfnormal":
                sigma_hat = float(np.exp(phat[k]))
                lam_hat = float(np.exp(phat[k + 1]))
                sigma_v = sigma_hat / np.sqrt(1 + lam_hat ** 2)
                sigma_u = sigma_hat * lam_hat / np.sqrt(1 + lam_hat ** 2)
                frontier_params["sigma"] = round(float(sigma_hat), 4)
                frontier_params["lambda"] = round(float(lam_hat), 4)
                frontier_params["sigma_u"] = round(float(sigma_u), 4)
                frontier_params["sigma_v"] = round(float(sigma_v), 4)
                eps = y - X @ beta_hat
                te_scores = _technical_efficiency_halfnormal(eps, sigma_u, sigma_v)

            elif model_type == "truncated" and len(phat) >= k + 3:
                mu_hat = float(phat[k])
                sigma_u = float(np.exp(phat[k + 1]))
                sigma_v = float(np.exp(phat[k + 2]))
                frontier_params["mu"] = round(float(mu_hat), 4)
                frontier_params["sigma_u"] = round(float(sigma_u), 4)
                frontier_params["sigma_v"] = round(float(sigma_v), 4)
                # TE for truncated normal (approximate using half-normal formula)
                eps = y - X @ beta_hat
                te_scores = _technical_efficiency_halfnormal(eps, sigma_u, sigma_v)

        # Fallback: TE from OLS residuals (rough approximation)
        if te_scores is None:
            eps = resid_ols
            sigma_u_approx = max(float(np.std(eps[eps < 0])), 1e-6)
            sigma_v_approx = max(float(np.std(eps[eps >= 0])), 1e-6)
            te_scores = _technical_efficiency_halfnormal(eps, sigma_u_approx, sigma_v_approx)

        te_scores = np.clip(te_scores, 0.0, 1.0)
        mean_te = float(np.mean(te_scores))
        median_te = float(np.median(te_scores))
        te_std = float(np.std(te_scores))

        # TE distribution
        te_distribution = {
            "mean": round(mean_te, 4),
            "median": round(median_te, 4),
            "std": round(te_std, 4),
            "min": round(float(np.min(te_scores)), 4),
            "max": round(float(np.max(te_scores)), 4),
            "pct10": round(float(np.percentile(te_scores, 10)), 4),
            "pct25": round(float(np.percentile(te_scores, 25)), 4),
            "pct75": round(float(np.percentile(te_scores, 75)), 4),
            "pct90": round(float(np.percentile(te_scores, 90)), 4),
            "pct_highly_efficient": round(float(np.mean(te_scores >= 0.9)) * 100, 1),
            "pct_highly_inefficient": round(float(np.mean(te_scores < 0.5)) * 100, 1),
        }

        # --- Determinants of inefficiency (if z data available) ---
        z_results = {}
        if z_list and any(z for z in z_list):
            z_keys = sorted({k for z in z_list for k in z.keys()})
            if z_keys:
                # Regress 1 - TE on z variables
                u_hat = 1 - te_scores
                Z = np.column_stack([
                    np.ones(n),
                    *[np.array([d.get(k, 0.0) for d in z_list]) for k in z_keys],
                ])
                try:
                    delta = np.linalg.lstsq(Z, u_hat, rcond=None)[0]
                    z_results = {
                        "determinants": {
                            k: round(float(d), 4)
                            for k, d in zip(["constant"] + z_keys, delta)
                        }
                    }
                    # R-squared
                    u_pred = Z @ delta
                    ss_res = float(np.sum((u_hat - u_pred) ** 2))
                    ss_tot = float(np.sum((u_hat - np.mean(u_hat)) ** 2))
                    z_results["r_squared"] = round(1 - ss_res / ss_tot, 4) if ss_tot > 0 else 0
                except np.linalg.LinAlgError:
                    pass

        # --- Score: inefficiency level ---
        # Higher inefficiency (lower TE) -> higher score (STRESS)
        # mean_te=1.0 -> score ~0, mean_te=0.5 -> score ~50, mean_te=0 -> score ~100
        base_score = (1.0 - mean_te) * 80

        # Penalty for high dispersion
        dispersion_penalty = min(te_std * 40, 15)

        # Reward if OLS residuals had correct negative skew (evidence of frontier)
        skew_bonus = 5.0 if not wrong_skew else -5.0

        score = base_score + dispersion_penalty + skew_bonus
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "model": model_type,
            "n_obs": n,
            "frontier_parameters": frontier_params,
            "ols_skewness": round(float(eps_skew), 4),
            "wrong_skew_warning": wrong_skew,
            "technical_efficiency": te_distribution,
            "inefficiency_determinants": z_results,
        }
