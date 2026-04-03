"""Oaxaca-Blinder wage gap decomposition.

Decomposes the mean wage gap between two groups (e.g. male/female, white/minority)
into explained (endowments) and unexplained (discrimination/coefficients) components.

Two-fold decomposition (Oaxaca 1973, Blinder 1973):
    ln(w_A) - ln(w_B) = (X_A - X_B)*beta_A + X_B*(beta_A - beta_B)
                       = Endowments     + Coefficients (unexplained)

Three-fold decomposition (Neumark 1988, Oaxaca & Ransom 1994):
    Gap = (X_A - X_B)*beta* + X_A*(beta_A - beta*) + X_B*(beta* - beta_B)
        = Endowments      + Advantage(A)          + Disadvantage(B)

where beta* is the non-discriminatory wage structure (pooled or weighted).

The unexplained component is an upper bound on discrimination (may include
unobserved productivity differences).

References:
    Oaxaca, R. (1973). Male-Female Wage Differentials in Urban Labor Markets.
        International Economic Review 14(3): 693-709.
    Blinder, A. (1973). Wage Discrimination: Reduced Form and Structural
        Estimates. Journal of Human Resources 8(4): 436-455.
    Neumark, D. (1988). Employers' Discriminatory Behavior and the Estimation
        of Wage Discrimination. Journal of Human Resources 23(3): 279-295.

Score: large unexplained gap (>20%) -> STRESS/CRISIS (discrimination signal),
small gap (<5%) -> STABLE.
"""

import numpy as np

from app.layers.base import LayerBase


class OaxacaBlinder(LayerBase):
    layer_id = "l3"
    name = "Oaxaca-Blinder Decomposition"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        year = kwargs.get("year")

        year_clause = "AND dp.date = ?" if year else ""
        params = [country, "wage_gap"]
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

        group_a_wages, group_a_X = [], []
        group_b_wages, group_b_X = [], []

        for row in rows:
            wage = row["value"]
            if wage is None or wage <= 0:
                continue
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            group = meta.get("group")
            schooling = meta.get("years_schooling")
            experience = meta.get("experience")
            if group is None or schooling is None or experience is None:
                continue

            features = [1.0, float(schooling), float(experience), float(experience) ** 2]

            if group == "A":
                group_a_wages.append(np.log(wage))
                group_a_X.append(features)
            elif group == "B":
                group_b_wages.append(np.log(wage))
                group_b_X.append(features)

        n_a, n_b = len(group_a_wages), len(group_b_wages)
        if n_a < 15 or n_b < 15:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient obs per group"}

        y_a = np.array(group_a_wages)
        y_b = np.array(group_b_wages)
        X_a = np.array(group_a_X)
        X_b = np.array(group_b_X)

        beta_a = np.linalg.lstsq(X_a, y_a, rcond=None)[0]
        beta_b = np.linalg.lstsq(X_b, y_b, rcond=None)[0]

        mean_X_a = X_a.mean(axis=0)
        mean_X_b = X_b.mean(axis=0)
        raw_gap = float(y_a.mean() - y_b.mean())

        # Two-fold decomposition (using group A coefficients as reference)
        endowments = float((mean_X_a - mean_X_b) @ beta_a)
        coefficients = float(mean_X_b @ (beta_a - beta_b))

        # Three-fold decomposition (pooled beta* as reference)
        X_pooled = np.vstack([X_a, X_b])
        y_pooled = np.concatenate([y_a, y_b])
        beta_star = np.linalg.lstsq(X_pooled, y_pooled, rcond=None)[0]

        endowments_3 = float((mean_X_a - mean_X_b) @ beta_star)
        advantage_a = float(mean_X_a @ (beta_a - beta_star))
        disadvantage_b = float(mean_X_b @ (beta_star - beta_b))

        # Score: unexplained component share of gap
        unexplained_share = abs(coefficients / raw_gap) if abs(raw_gap) > 1e-6 else 0.0
        score = min(100.0, max(0.0, unexplained_share * 100.0))

        coef_names = ["constant", "schooling", "experience", "experience_sq"]

        return {
            "score": round(score, 2),
            "country": country,
            "n_group_a": n_a,
            "n_group_b": n_b,
            "raw_gap": round(raw_gap, 4),
            "two_fold": {
                "endowments": round(endowments, 4),
                "coefficients_unexplained": round(coefficients, 4),
                "endowments_share": round(endowments / raw_gap, 4) if abs(raw_gap) > 1e-6 else None,
                "unexplained_share": round(coefficients / raw_gap, 4) if abs(raw_gap) > 1e-6 else None,
            },
            "three_fold": {
                "endowments": round(endowments_3, 4),
                "advantage_a": round(advantage_a, 4),
                "disadvantage_b": round(disadvantage_b, 4),
            },
            "coefficients_group_a": dict(zip(coef_names, beta_a.tolist())),
            "coefficients_group_b": dict(zip(coef_names, beta_b.tolist())),
            "coefficients_pooled": dict(zip(coef_names, beta_star.tolist())),
        }
