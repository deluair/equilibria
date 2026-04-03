"""Union wage premium estimation with selection correction.

The union wage premium is the percentage wage advantage of union members over
comparable non-union workers. Raw differentials overstate the premium due to
selection: unionized jobs concentrate in higher-paying industries and workers
who select into unions may differ in unobservable productivity.

Heckman (1979) two-step correction:
    1. Probit for union membership:
       P(union=1|Z) = Phi(Z*gamma)
    2. Wage equation with inverse Mills ratio (lambda):
       ln(w) = X*beta + delta*union + rho*lambda + e

where lambda = phi(Z*gamma)/Phi(Z*gamma) for union members and
-phi(Z*gamma)/(1-Phi(Z*gamma)) for non-union.

Typical findings:
    - US union premium: ~15-20% raw, ~10-15% after controls (Freeman & Medoff 1984)
    - Premium has been stable even as unionization declined (Hirsch 2004)
    - Larger premium for less-educated, minority workers
    - Public sector premium > private sector

Declining unionization effects:
    Aggregate wage inequality increases as unionization falls (Card 2001,
    Western & Rosenfeld 2011). Counterfactual: US inequality would be
    lower if unionization had stayed at 1973 levels.

References:
    Freeman, R. & Medoff, J. (1984). What Do Unions Do? Basic Books.
    Heckman, J. (1979). Sample Selection Bias as a Specification Error.
        Econometrica 47(1): 153-161.
    Card, D. (2001). The Effect of Unions on Wage Inequality in the US
        Labor Market. ILR Review 54(2): 296-315.

Score: high premium with declining union density -> STRESS (rising inequality).
Moderate premium with stable density -> STABLE.
"""

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class UnionWagePremium(LayerBase):
    layer_id = "l3"
    name = "Union Wage Premium"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        year = kwargs.get("year")

        year_clause = "AND dp.date = ?" if year else ""
        params = [country, "union_wage"]
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

        if not rows or len(rows) < 30:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient wage data"}

        import json

        ln_wages = []
        union_status = []
        covariates = []
        selection_vars = []

        for row in rows:
            wage = row["value"]
            if wage is None or wage <= 0:
                continue
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            union = meta.get("union_member")
            educ = meta.get("years_schooling")
            exp = meta.get("experience")
            industry_size = meta.get("industry_size")
            if union is None or educ is None or exp is None:
                continue

            ln_wages.append(np.log(wage))
            union_status.append(int(union))
            covariates.append([float(educ), float(exp), float(exp) ** 2])
            # Selection equation extra variables
            selection_vars.append([
                float(educ), float(exp),
                float(industry_size) if industry_size is not None else 0.0,
            ])

        n = len(ln_wages)
        if n < 30:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid obs"}

        y = np.array(ln_wages)
        d = np.array(union_status)
        X_cov = np.array(covariates)
        Z = np.array(selection_vars)

        # Raw premium (simple difference in means)
        union_mask = d == 1
        non_union_mask = d == 0
        if np.sum(union_mask) < 5 or np.sum(non_union_mask) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient obs per group"}

        raw_premium = float(np.mean(y[union_mask]) - np.mean(y[non_union_mask]))

        # OLS premium (controlling for covariates)
        X_ols = np.column_stack([np.ones(n), d, X_cov])
        beta_ols = np.linalg.lstsq(X_ols, y, rcond=None)[0]
        ols_premium = float(beta_ols[1])

        # Robust SE for OLS
        resid_ols = y - X_ols @ beta_ols
        n_k = n - X_ols.shape[1]
        XtX_inv = np.linalg.pinv(X_ols.T @ X_ols)
        omega = np.diag(resid_ols ** 2) * (n / max(n_k, 1))
        V_ols = XtX_inv @ (X_ols.T @ omega @ X_ols) @ XtX_inv
        se_ols = np.sqrt(np.maximum(np.diag(V_ols), 0.0))

        # Heckman selection correction
        # Step 1: Probit for union membership
        Z_full = np.column_stack([np.ones(n), Z])
        gamma, imr = self._probit_selection(Z_full, d)

        # Step 2: Wage equation with IMR
        X_heck = np.column_stack([np.ones(n), d, X_cov, imr])
        beta_heck = np.linalg.lstsq(X_heck, y, rcond=None)[0]
        heckman_premium = float(beta_heck[1])
        selection_coef = float(beta_heck[-1])  # coefficient on IMR

        # Union density
        union_density = float(np.mean(d))

        # Score: high premium + declining density -> inequality stress
        if ols_premium > 0.20:
            score = 50.0 + (ols_premium - 0.20) * 150.0
        elif ols_premium > 0.10:
            score = 25.0 + (ols_premium - 0.10) * 250.0
        else:
            score = ols_premium * 250.0
        # Amplify if low union density (inequality concentrating)
        if union_density < 0.10:
            score = min(100.0, score * 1.3)
        score = max(0.0, min(100.0, score))

        coef_names_ols = ["constant", "union", "schooling", "experience", "experience_sq"]

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "n_union": int(np.sum(union_mask)),
            "n_non_union": int(np.sum(non_union_mask)),
            "union_density": round(union_density, 4),
            "raw_premium": round(raw_premium, 4),
            "ols_premium": round(ols_premium, 4),
            "ols_se": round(float(se_ols[1]), 4),
            "heckman_premium": round(heckman_premium, 4),
            "selection_correction": {
                "lambda_coefficient": round(selection_coef, 4),
                "selection_bias": "positive" if selection_coef > 0 else "negative",
                "bias_magnitude": round(abs(ols_premium - heckman_premium), 4),
            },
            "ols_coefficients": dict(zip(coef_names_ols, beta_ols.tolist())),
        }

    @staticmethod
    def _probit_selection(Z: np.ndarray, d: np.ndarray, max_iter: int = 30, tol: float = 1e-8):
        """Probit estimation via IRLS for Heckman first stage.

        Returns gamma (probit coefficients) and inverse Mills ratio for each obs.
        """
        n, k = Z.shape
        gamma = np.zeros(k)

        for _ in range(max_iter):
            eta = Z @ gamma
            eta = np.clip(eta, -10, 10)
            Phi = sp_stats.norm.cdf(eta)
            phi = sp_stats.norm.pdf(eta)

            # Avoid division by zero
            Phi = np.clip(Phi, 1e-10, 1 - 1e-10)

            # Score and Hessian
            ratio = phi / (Phi * (1 - Phi))
            w = ratio * phi
            (d - Phi) / (Phi * (1 - Phi)) * phi

            W = np.diag(np.clip(w, 1e-10, 1e10))
            ZtWZ = Z.T @ W @ Z
            try:
                step = np.linalg.solve(ZtWZ, Z.T @ (d - Phi) * phi)
            except np.linalg.LinAlgError:
                step = np.linalg.lstsq(ZtWZ, Z.T @ ((d - Phi) * phi), rcond=None)[0]

            gamma_new = gamma + step
            if np.max(np.abs(gamma_new - gamma)) < tol:
                gamma = gamma_new
                break
            gamma = gamma_new

        # Inverse Mills ratio
        eta = Z @ gamma
        eta = np.clip(eta, -10, 10)
        Phi = np.clip(sp_stats.norm.cdf(eta), 1e-10, 1 - 1e-10)
        phi = sp_stats.norm.pdf(eta)

        imr = np.where(d == 1, phi / Phi, -phi / (1 - Phi))

        return gamma, imr
