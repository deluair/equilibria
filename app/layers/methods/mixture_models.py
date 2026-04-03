"""Finite mixture models and convergence clubs (McLachlan & Peel 2000).

Finite mixture models assume the observed data arise from a mixture of K
unobserved subpopulations (components), each with its own parametric
distribution. The EM algorithm iterates between:
    E-step: compute posterior probabilities of component membership
    M-step: update component parameters (means, variances, mixing proportions)

Key applications in economics:
    - Convergence clubs (Durlauf & Johnson 1995): groups of countries converging
      to different steady states, identified as mixture components
    - Latent class regression: heterogeneous treatment effects with discrete
      unobserved types (e.g. compliers vs never-takers)
    - Income distribution analysis: mixtures capturing multi-modal earnings

Model selection via BIC/AIC determines optimal number of components.

References:
    McLachlan, G. & Peel, D. (2000). Finite Mixture Models. Wiley.
    Durlauf, S. & Johnson, P. (1995). Multiple Regimes and Cross-Country
        Growth Behaviour. Journal of Applied Econometrics 10(4): 365-384.
    Phillips, P. & Sul, D. (2007). Transition Modeling and Econometric
        Convergence Tests. Econometrica 75(6): 1771-1855.

Score: many distinct clusters (high K, well-separated) -> high score
(heterogeneity/divergence). Single cluster -> STABLE (convergence).
"""

import json

import numpy as np

from app.layers.base import LayerBase


class MixtureModels(LayerBase):
    layer_id = "l18"
    name = "Mixture Models"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        max_components = kwargs.get("max_components", 5)

        rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata, ds.country_iso3
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'mixture'
            ORDER BY dp.date DESC
            """,
            (),
        )

        if not rows or len(rows) < 20:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = np.array([float(r["value"]) for r in rows if r["value"] is not None])
        n = len(values)
        if n < 20:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid obs"}

        # Parse labels (country names) if available for convergence clubs
        labels = []
        for r in rows:
            if r["value"] is None:
                continue
            meta = json.loads(r["metadata"]) if r.get("metadata") else {}
            labels.append(meta.get("label", r.get("country_iso3", "")))

        # Fit mixture models for K = 1 to max_components
        results_by_k = {}
        for K in range(1, max_components + 1):
            em_result = self._em_gaussian(values, K, max_iter=100, tol=1e-6)
            if em_result is None:
                continue
            ll = em_result["log_likelihood"]
            n_params = 3 * K - 1  # K means + K variances + (K-1) weights
            bic = -2 * ll + n_params * np.log(n)
            aic = -2 * ll + 2 * n_params
            results_by_k[K] = {
                "log_likelihood": round(ll, 4),
                "bic": round(bic, 4),
                "aic": round(aic, 4),
                "components": em_result["components"],
                "converged": em_result["converged"],
                "iterations": em_result["iterations"],
            }

        if not results_by_k:
            return {"score": None, "signal": "UNAVAILABLE", "error": "EM failed for all K"}

        # Select optimal K by BIC
        optimal_k = min(results_by_k, key=lambda k: results_by_k[k]["bic"])
        best = results_by_k[optimal_k]

        # Convergence clubs: assign each observation to its most likely component
        clubs = {}
        if optimal_k > 1:
            posteriors = self._compute_posteriors(values, best["components"])
            assignments = np.argmax(posteriors, axis=1)
            for club_id in range(optimal_k):
                members = [labels[i] for i in range(n) if assignments[i] == club_id]
                club_vals = values[assignments == club_id]
                clubs[f"club_{club_id + 1}"] = {
                    "n_members": len(members),
                    "mean": round(float(np.mean(club_vals)), 4) if len(club_vals) > 0 else None,
                    "std": round(float(np.std(club_vals)), 4) if len(club_vals) > 0 else None,
                    "members_sample": members[:10],
                }

        # Latent class regression: if covariates available, estimate class-specific betas
        lcr = None
        x_data = []
        for r in rows:
            if r["value"] is None:
                continue
            meta = json.loads(r["metadata"]) if r.get("metadata") else {}
            covars = meta.get("covariates", {})
            if covars:
                x_data.append(covars)
        if len(x_data) == n and x_data[0]:
            x_keys = sorted(x_data[0].keys())
            X = np.column_stack([
                np.array([d.get(k, 0.0) for d in x_data]) for k in x_keys
            ])
            lcr = self._latent_class_regression(X, values, optimal_k, best["components"])

        # Score: more distinct clubs -> higher score
        if optimal_k >= 4:
            score = 75.0
        elif optimal_k == 3:
            score = 55.0
        elif optimal_k == 2:
            # Check separation
            if len(best["components"]) == 2:
                m1 = best["components"][0]["mean"]
                m2 = best["components"][1]["mean"]
                s1 = best["components"][0]["std"]
                s2 = best["components"][1]["std"]
                separation = abs(m1 - m2) / max(s1 + s2, 1e-10)
                score = 25.0 + min(separation * 10.0, 30.0)
            else:
                score = 35.0
        else:
            score = 10.0
        score = max(0.0, min(100.0, score))

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "optimal_k": optimal_k,
            "model_selection": {
                k: {"bic": v["bic"], "aic": v["aic"]}
                for k, v in results_by_k.items()
            },
            "components": best["components"],
            "convergence_clubs": clubs if clubs else None,
            "latent_class_regression": lcr,
        }

    @staticmethod
    def _em_gaussian(data: np.ndarray, K: int, max_iter: int = 100,
                     tol: float = 1e-6) -> dict | None:
        """Expectation-Maximization for Gaussian mixture."""
        n = len(data)
        if K == 1:
            mu = np.mean(data)
            sigma2 = np.var(data)
            ll = float(np.sum(-0.5 * np.log(2 * np.pi * sigma2) - (data - mu) ** 2 / (2 * sigma2)))
            return {
                "log_likelihood": ll,
                "components": [{"mean": round(float(mu), 4), "std": round(float(np.sqrt(sigma2)), 4),
                                "weight": 1.0}],
                "converged": True,
                "iterations": 1,
            }

        # Initialize with quantile-based means
        percentiles = np.linspace(100 / (K + 1), 100 * K / (K + 1), K)
        mu = np.percentile(data, percentiles)
        sigma2 = np.full(K, np.var(data))
        pi_k = np.full(K, 1.0 / K)

        prev_ll = -np.inf
        for iteration in range(max_iter):
            # E-step: posterior probabilities
            gamma = np.zeros((n, K))
            for k in range(K):
                if sigma2[k] <= 1e-15:
                    sigma2[k] = 1e-10
                gamma[:, k] = pi_k[k] * np.exp(
                    -0.5 * np.log(2 * np.pi * sigma2[k])
                    - (data - mu[k]) ** 2 / (2 * sigma2[k])
                )
            row_sums = gamma.sum(axis=1, keepdims=True)
            row_sums[row_sums < 1e-300] = 1e-300
            gamma /= row_sums

            # M-step
            N_k = gamma.sum(axis=0)
            N_k[N_k < 1e-10] = 1e-10
            for k in range(K):
                mu[k] = float(np.sum(gamma[:, k] * data) / N_k[k])
                sigma2[k] = float(np.sum(gamma[:, k] * (data - mu[k]) ** 2) / N_k[k])
                sigma2[k] = max(sigma2[k], 1e-10)
                pi_k[k] = float(N_k[k] / n)

            # Log-likelihood
            ll_terms = np.zeros(n)
            for k in range(K):
                ll_terms += pi_k[k] * np.exp(
                    -0.5 * np.log(2 * np.pi * sigma2[k])
                    - (data - mu[k]) ** 2 / (2 * sigma2[k])
                )
            ll_terms[ll_terms < 1e-300] = 1e-300
            ll = float(np.sum(np.log(ll_terms)))

            if abs(ll - prev_ll) < tol:
                components = []
                for k in range(K):
                    components.append({
                        "mean": round(float(mu[k]), 4),
                        "std": round(float(np.sqrt(sigma2[k])), 4),
                        "weight": round(float(pi_k[k]), 4),
                    })
                return {
                    "log_likelihood": ll,
                    "components": sorted(components, key=lambda c: c["mean"]),
                    "converged": True,
                    "iterations": iteration + 1,
                }
            prev_ll = ll

        # Did not converge but return last result
        components = []
        for k in range(K):
            components.append({
                "mean": round(float(mu[k]), 4),
                "std": round(float(np.sqrt(sigma2[k])), 4),
                "weight": round(float(pi_k[k]), 4),
            })
        return {
            "log_likelihood": float(prev_ll),
            "components": sorted(components, key=lambda c: c["mean"]),
            "converged": False,
            "iterations": max_iter,
        }

    @staticmethod
    def _compute_posteriors(data: np.ndarray, components: list) -> np.ndarray:
        """Compute posterior class probabilities."""
        n = len(data)
        K = len(components)
        gamma = np.zeros((n, K))
        for k, comp in enumerate(components):
            mu = comp["mean"]
            sigma = max(comp["std"], 1e-10)
            w = comp["weight"]
            gamma[:, k] = w * np.exp(-0.5 * ((data - mu) / sigma) ** 2) / (sigma * np.sqrt(2 * np.pi))
        row_sums = gamma.sum(axis=1, keepdims=True)
        row_sums[row_sums < 1e-300] = 1e-300
        return gamma / row_sums

    @staticmethod
    def _latent_class_regression(X: np.ndarray, y: np.ndarray, K: int,
                                 components: list) -> dict | None:
        """Class-specific OLS regressions weighted by posterior membership."""
        posteriors = MixtureModels._compute_posteriors(y, components)
        class_betas = {}
        for k in range(K):
            weights = posteriors[:, k]
            if np.sum(weights) < 1.0:
                continue
            W = np.diag(weights)
            try:
                beta = np.linalg.solve(X.T @ W @ X, X.T @ W @ y)
                class_betas[f"class_{k + 1}"] = [round(float(b), 4) for b in beta]
            except np.linalg.LinAlgError:
                continue
        return class_betas if class_betas else None
