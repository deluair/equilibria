"""Gravity model for bilateral migration flows.

Adapts the trade gravity framework to international migration. Bilateral
migration stocks/flows are modeled as a function of economic mass (population,
income), geographic and cultural distance, and institutional factors.

Specification:
    ln(M_ij) = b0 + b1*ln(pop_i) + b2*ln(pop_j) + b3*ln(gdppc_i)
               + b4*ln(gdppc_j) + b5*ln(dist_ij) + b6*lang_ij
               + b7*colony_ij + b8*visa_ij + e_ij

where M_ij is migration stock from origin i to destination j.

Key findings in literature:
    - Income differential is the primary pull factor (Borjas 1987)
    - Distance captures information costs, not just transport (Beine et al. 2011)
    - Colonial/language ties reduce cultural distance (Grogger & Hanson 2011)
    - Network effects: existing diaspora lowers migration costs

PPML preferred over log-linear OLS for same reasons as trade gravity:
handles zeros, consistent under heteroskedasticity (Santos Silva & Tenreyro 2006).

References:
    Beine, M., Bertoli, S. & Fernandez-Huertas Moraga, J. (2016). A
        Practitioners' Guide to Gravity Models of International Migration.
        World Economy 39(4): 496-512.
    Grogger, J. & Hanson, G. (2011). Income Maximization and the Selection
        and Sorting of International Migrants. Journal of Development
        Economics 95(1): 42-57.
    Borjas, G. (1987). Self-Selection and the Earnings of Immigrants.
        American Economic Review 77(4): 531-553.

Score: model fit deviation. Poor fit -> STRESS (unusual migration patterns).
"""

import numpy as np

from app.layers.base import LayerBase


class MigrationGravity(LayerBase):
    layer_id = "l3"
    name = "Migration Gravity Model"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        year = kwargs.get("year")

        year_clause = "AND dp.date = ?" if year else ""
        params = [country, "migration_gravity"]
        if year:
            params.append(str(year))

        rows = await db.fetch_all(
            f"""
            SELECT dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = ?
              {year_clause}
            ORDER BY dp.date DESC
            """,
            tuple(params),
        )

        if not rows or len(rows) < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient migration data"}

        import json

        mig_flows = []
        features = []

        for row in rows:
            flow = row["value"]
            if flow is None or flow <= 0:
                continue
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            pop_o = meta.get("pop_origin")
            pop_d = meta.get("pop_dest")
            gdppc_o = meta.get("gdppc_origin")
            gdppc_d = meta.get("gdppc_dest")
            dist = meta.get("distance")
            if not all([pop_o, pop_d, gdppc_o, gdppc_d, dist]):
                continue
            if any(v <= 0 for v in [pop_o, pop_d, gdppc_o, gdppc_d, dist]):
                continue

            mig_flows.append(flow)
            features.append([
                1.0,
                np.log(pop_o),
                np.log(pop_d),
                np.log(gdppc_o),
                np.log(gdppc_d),
                np.log(dist),
                float(meta.get("common_language", 0)),
                float(meta.get("colonial_tie", 0)),
                float(meta.get("visa_free", 0)),
            ])

        n = len(mig_flows)
        if n < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid obs"}

        y = np.array(mig_flows)
        ln_y = np.log(y)
        X = np.array(features)

        # OLS on log-linear
        beta_ols = np.linalg.lstsq(X, ln_y, rcond=None)[0]
        resid_ols = ln_y - X @ beta_ols
        ss_res = np.sum(resid_ols ** 2)
        ss_tot = np.sum((ln_y - ln_y.mean()) ** 2)
        r2_ols = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        # PPML via IRLS
        beta_ppml, pseudo_r2, iterations = self._ppml(X, y)

        # Income elasticity: response of migration to destination income
        income_elasticity = float(beta_ppml[4]) if beta_ppml is not None else float(beta_ols[4])
        distance_elasticity = float(beta_ppml[5]) if beta_ppml is not None else float(beta_ols[5])

        # Score based on model fit
        fit_metric = pseudo_r2 if beta_ppml is not None else r2_ols
        score = max(0.0, min(100.0, (1.0 - fit_metric) * 100.0))

        coef_names = [
            "constant", "ln_pop_origin", "ln_pop_dest", "ln_gdppc_origin",
            "ln_gdppc_dest", "ln_distance", "common_language", "colonial_tie", "visa_free",
        ]

        result = {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "ols": {
                "coefficients": dict(zip(coef_names, beta_ols.tolist())),
                "r_squared": round(r2_ols, 4),
            },
            "elasticities": {
                "income_destination": round(income_elasticity, 4),
                "distance": round(distance_elasticity, 4),
                "income_differential_pull": income_elasticity > 0 and distance_elasticity < 0,
            },
        }

        if beta_ppml is not None:
            result["ppml"] = {
                "coefficients": dict(zip(coef_names, beta_ppml.tolist())),
                "pseudo_r2": round(pseudo_r2, 4),
                "iterations": iterations,
            }

        return result

    @staticmethod
    def _ppml(X: np.ndarray, y: np.ndarray, max_iter: int = 50, tol: float = 1e-8):
        """PPML via IRLS for migration gravity."""
        n, k = X.shape
        beta = np.zeros(k)
        beta[0] = np.log(np.mean(y)) if np.mean(y) > 0 else 0.0

        for i in range(max_iter):
            mu = np.exp(X @ beta)
            mu = np.clip(mu, 1e-10, 1e20)
            z = X @ beta + (y - mu) / mu
            W = mu
            XtWX = X.T @ (X * W[:, None])
            XtWz = X.T @ (W * z)
            try:
                beta_new = np.linalg.solve(XtWX, XtWz)
            except np.linalg.LinAlgError:
                return None, 0.0, i + 1
            if np.max(np.abs(beta_new - beta)) < tol:
                beta = beta_new
                mu = np.exp(X @ beta)
                mu = np.clip(mu, 1e-10, 1e20)
                mask = y > 0
                dev_full = 2.0 * np.sum(y[mask] * np.log(y[mask] / mu[mask]) - (y[mask] - mu[mask]))
                mu_null = np.mean(y)
                dev_null = 2.0 * np.sum(y[mask] * np.log(y[mask] / mu_null) - (y[mask] - mu_null))
                pr2 = 1.0 - dev_full / dev_null if dev_null > 0 else 0.0
                return beta, max(0.0, min(1.0, pr2)), i + 1
            beta = beta_new

        return beta, 0.0, max_iter
