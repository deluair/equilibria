"""Gravity model of international trade.

Estimates the gravity equation -- the workhorse of empirical trade -- using both
log-linear OLS (Santos Silva & Tenreyro 2006 baseline comparison) and Poisson
Pseudo-Maximum Likelihood (PPML).  The gravity equation relates bilateral trade
flows to the economic mass of trading partners (GDP) and trade frictions
(distance, borders, shared language, colonial ties).

Standard specification:
    ln(X_ij) = b0 + b1*ln(GDP_i) + b2*ln(GDP_j) + b3*ln(dist_ij)
               + b4*contig_ij + b5*comlang_ij + b6*colony_ij + e_ij

PPML specification (preferred, handles zeros and heteroskedasticity):
    X_ij = exp(b0 + b1*ln(GDP_i) + ... + b6*colony_ij) * eta_ij

The score (0-100) reflects model fit deviation: low R-squared or large prediction
errors push the score toward STRESS/CRISIS, indicating the standard gravity
framework poorly describes the country's trade pattern (structural anomaly).
"""

import numpy as np

from app.layers.base import LayerBase


class GravityModel(LayerBase):
    layer_id = "l1"
    name = "Gravity Model"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        year = kwargs.get("year")

        # Fetch bilateral trade data with gravity variables
        year_clause = "AND dp.date = ?" if year else ""
        params = [country, country]
        if year:
            params.append(str(year))

        rows = await db.fetch_all(
            f"""
            SELECT
                dp.value AS trade_value,
                ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'gravity'
              AND (ds.country_iso3 = ? OR ds.description LIKE '%' || ? || '%')
              {year_clause}
            ORDER BY dp.date DESC
            """,
            tuple(params),
        )

        if not rows or len(rows) < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient gravity data"}

        # Parse gravity variables from metadata JSON stored in series
        import json

        trade_vals = []
        ln_gdp_i = []
        ln_gdp_j = []
        ln_dist = []
        contig = []
        comlang = []
        colony = []

        for row in rows:
            tv = row["trade_value"]
            if tv is None or tv <= 0:
                continue
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            gdp_i = meta.get("gdp_origin")
            gdp_j = meta.get("gdp_dest")
            dist = meta.get("distance")
            if not all([gdp_i, gdp_j, dist]):
                continue
            if gdp_i <= 0 or gdp_j <= 0 or dist <= 0:
                continue

            trade_vals.append(tv)
            ln_gdp_i.append(np.log(gdp_i))
            ln_gdp_j.append(np.log(gdp_j))
            ln_dist.append(np.log(dist))
            contig.append(float(meta.get("contiguity", 0)))
            comlang.append(float(meta.get("common_language", 0)))
            colony.append(float(meta.get("colonial_tie", 0)))

        n = len(trade_vals)
        if n < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid obs"}

        y = np.array(trade_vals)
        ln_y = np.log(y)

        # Build design matrix [const, ln_gdp_i, ln_gdp_j, ln_dist, contig, comlang, colony]
        X = np.column_stack([
            np.ones(n),
            np.array(ln_gdp_i),
            np.array(ln_gdp_j),
            np.array(ln_dist),
            np.array(contig),
            np.array(comlang),
            np.array(colony),
        ])

        # OLS on log-linear specification
        ols_result = self._ols(X, ln_y)

        # PPML via iteratively reweighted least squares
        ppml_result = self._ppml(X, y, max_iter=50, tol=1e-8)

        # Predicted vs actual
        ols_predicted = np.exp(X @ ols_result["coefficients"])
        ppml_predicted = np.exp(X @ ppml_result["coefficients"])

        # Score: based on PPML pseudo-R-squared
        # High R2 -> low score (STABLE), low R2 -> high score (anomalous trade pattern)
        pseudo_r2 = ppml_result["pseudo_r2"]
        score = max(0.0, min(100.0, (1.0 - pseudo_r2) * 100.0))

        coef_names = ["constant", "ln_gdp_origin", "ln_gdp_dest", "ln_distance",
                      "contiguity", "common_language", "colonial_tie"]

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "ols": {
                "coefficients": dict(zip(coef_names, ols_result["coefficients"].tolist())),
                "std_errors": dict(zip(coef_names, ols_result["std_errors"].tolist())),
                "r_squared": round(ols_result["r_squared"], 4),
            },
            "ppml": {
                "coefficients": dict(zip(coef_names, ppml_result["coefficients"].tolist())),
                "std_errors": dict(zip(coef_names, ppml_result["std_errors"].tolist())),
                "pseudo_r2": round(ppml_result["pseudo_r2"], 4),
                "iterations": ppml_result["iterations"],
            },
            "diagnostics": {
                "mean_trade_value": round(float(np.mean(y)), 2),
                "median_trade_value": round(float(np.median(y)), 2),
                "ols_rmse": round(float(np.sqrt(np.mean((y - ols_predicted) ** 2))), 2),
                "ppml_rmse": round(float(np.sqrt(np.mean((y - ppml_predicted) ** 2))), 2),
            },
        }

    @staticmethod
    def _ols(X: np.ndarray, y: np.ndarray) -> dict:
        """Ordinary Least Squares with heteroskedasticity-robust (HC1) standard errors."""
        n, k = X.shape
        beta = np.linalg.lstsq(X, y, rcond=None)[0]
        resid = y - X @ beta
        ss_res = float(np.sum(resid ** 2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        # HC1 robust standard errors
        XtX_inv = np.linalg.inv(X.T @ X)
        # Diagonal matrix of squared residuals
        omega = np.diag(resid ** 2) * (n / (n - k))
        V = XtX_inv @ (X.T @ omega @ X) @ XtX_inv
        se = np.sqrt(np.diag(V))

        return {"coefficients": beta, "std_errors": se, "r_squared": r2}

    @staticmethod
    def _ppml(X: np.ndarray, y: np.ndarray, max_iter: int = 50, tol: float = 1e-8) -> dict:
        """Poisson Pseudo-Maximum Likelihood via IRLS.

        Santos Silva & Tenreyro (2006) showed PPML is consistent under
        heteroskedasticity and naturally handles zero trade flows.
        """
        n, k = X.shape
        beta = np.zeros(k)
        # Initialize with small values
        beta[0] = np.log(np.mean(y)) if np.mean(y) > 0 else 0.0

        converged = False
        iterations = 0
        for i in range(max_iter):
            mu = np.exp(X @ beta)
            mu = np.clip(mu, 1e-10, 1e20)
            # Working dependent variable
            z = X @ beta + (y - mu) / mu
            # Weight matrix (diagonal of mu)
            W = mu
            # Weighted least squares step
            XtWX = X.T @ (X * W[:, None])
            XtWz = X.T @ (W * z)
            try:
                beta_new = np.linalg.solve(XtWX, XtWz)
            except np.linalg.LinAlgError:
                break
            if np.max(np.abs(beta_new - beta)) < tol:
                converged = True
                beta = beta_new
                iterations = i + 1
                break
            beta = beta_new
            iterations = i + 1

        mu = np.exp(X @ beta)
        mu = np.clip(mu, 1e-10, 1e20)

        # Pseudo R-squared (deviance-based)
        # D = 2 * sum(y * ln(y/mu) - (y - mu)) for y > 0
        mask = y > 0
        dev_full = 2.0 * np.sum(y[mask] * np.log(y[mask] / mu[mask]) - (y[mask] - mu[mask]))
        mu_null = np.mean(y)
        dev_null = 2.0 * np.sum(y[mask] * np.log(y[mask] / mu_null) - (y[mask] - mu_null))
        pseudo_r2 = 1.0 - dev_full / dev_null if dev_null > 0 else 0.0

        # Robust standard errors (sandwich)
        XtWX = X.T @ (X * mu[:, None])
        try:
            XtWX_inv = np.linalg.inv(XtWX)
        except np.linalg.LinAlgError:
            XtWX_inv = np.linalg.pinv(XtWX)
        resid = y - mu
        B = X.T @ (X * (resid ** 2)[:, None])
        V = XtWX_inv @ B @ XtWX_inv
        se = np.sqrt(np.maximum(np.diag(V), 0.0))

        return {
            "coefficients": beta,
            "std_errors": se,
            "pseudo_r2": max(0.0, min(1.0, pseudo_r2)),
            "iterations": iterations,
            "converged": converged,
        }
