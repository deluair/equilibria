"""Mincer wage equation estimation.

The Mincer (1974) human capital earnings function is the foundational model in
labor economics for estimating returns to education and experience:

    ln(w_i) = b0 + b1*S_i + b2*X_i + b3*X_i^2 + e_i

where w is hourly wage, S is years of schooling, X is potential experience
(age - schooling - 6), and the quadratic in experience captures the concave
experience-earnings profile.

Key parameters:
    b1 = rate of return to an additional year of schooling (~6-12%)
    b2, b3 = experience profile: wages rise then flatten/decline
    peak experience = -b2 / (2*b3)

OLS estimates are biased upward due to ability bias (Card 2001). The module
estimates with heteroskedasticity-robust (HC1) standard errors.

References:
    Mincer, J. (1974). Schooling, Experience, and Earnings.
    Card, D. (2001). Estimating the Return to Schooling. Econometrica 69(5).
    Lemieux, T. (2006). The "Mincer Equation" Thirty Years After Schooling,
        Experience, and Earnings. In Jacob Mincer: A Pioneer of Modern Labor
        Economics, pp. 127-145.

Score: based on education premium. High premium (>15%) -> STRESS (inequality),
low premium (<5%) -> STABLE (compressed wages), moderate -> WATCH.
"""

import numpy as np

from app.layers.base import LayerBase


class MincerWageEquation(LayerBase):
    layer_id = "l3"
    name = "Mincer Wage Equation"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        year = kwargs.get("year")

        year_clause = "AND dp.date = ?" if year else ""
        params = [country, "mincer"]
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
        schooling = []
        experience = []
        experience_sq = []

        for row in rows:
            wage = row["value"]
            if wage is None or wage <= 0:
                continue
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            s = meta.get("years_schooling")
            age = meta.get("age")
            if s is None or age is None:
                continue
            s = float(s)
            age = float(age)
            x = max(age - s - 6.0, 0.0)

            ln_wages.append(np.log(wage))
            schooling.append(s)
            experience.append(x)
            experience_sq.append(x ** 2)

        n = len(ln_wages)
        if n < 30:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid obs"}

        y = np.array(ln_wages)
        X = np.column_stack([
            np.ones(n),
            np.array(schooling),
            np.array(experience),
            np.array(experience_sq),
        ])

        beta, se, r2 = self._ols_robust(X, y)

        education_premium = beta[1]
        peak_experience = -beta[2] / (2.0 * beta[3]) if abs(beta[3]) > 1e-12 else float("inf")

        # Score: education premium magnitude maps to labor stress
        # Very high premium (>0.15) signals inequality / skill scarcity
        # Very low premium (<0.03) signals compressed wages / low returns
        abs_premium = abs(education_premium)
        if abs_premium > 0.15:
            score = 50.0 + (abs_premium - 0.15) * 200.0  # scales into STRESS/CRISIS
        elif abs_premium < 0.03:
            score = 10.0  # STABLE but unusually low
        else:
            score = 20.0 + (abs_premium - 0.03) * 250.0  # 20-50 range for WATCH
        score = max(0.0, min(100.0, score))

        coef_names = ["constant", "schooling", "experience", "experience_sq"]

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "coefficients": dict(zip(coef_names, beta.tolist())),
            "std_errors": dict(zip(coef_names, se.tolist())),
            "r_squared": round(r2, 4),
            "education_premium": round(education_premium, 4),
            "peak_experience_years": round(peak_experience, 1) if peak_experience < 100 else None,
            "experience_profile": {
                "linear": round(beta[2], 4),
                "quadratic": round(beta[3], 6),
                "concave": beta[3] < 0,
            },
        }

    @staticmethod
    def _ols_robust(X: np.ndarray, y: np.ndarray) -> tuple:
        """OLS with HC1 heteroskedasticity-robust standard errors."""
        n, k = X.shape
        beta = np.linalg.lstsq(X, y, rcond=None)[0]
        resid = y - X @ beta
        ss_res = float(np.sum(resid ** 2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        XtX_inv = np.linalg.inv(X.T @ X)
        omega = np.diag(resid ** 2) * (n / (n - k))
        V = XtX_inv @ (X.T @ omega @ X) @ XtX_inv
        se = np.sqrt(np.maximum(np.diag(V), 0.0))

        return beta, se, r2
