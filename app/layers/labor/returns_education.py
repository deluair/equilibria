"""Instrumental variable estimation of returns to education.

OLS estimates of the Mincer equation suffer from ability bias: unobserved
ability is positively correlated with both schooling and wages, biasing the
schooling coefficient upward. IV estimation addresses this using instruments
that affect schooling but not wages directly.

Classical instruments:
    - Distance to nearest college (Card 1995): affects schooling cost
    - Compulsory schooling laws (Angrist & Krueger 1991): quarter of birth x
      compulsory attendance laws as excluded instruments
    - Twin/sibling studies (Ashenfelter & Krueger 1994)

Two-stage least squares (2SLS):
    First stage:  S_i = pi_0 + pi_1*Z_i + pi_2*X_i + v_i
    Second stage: ln(w_i) = b0 + b1*S_hat_i + b2*X_i + e_i

Diagnostics:
    - First-stage F-statistic (>10 rule of thumb, Stock & Yogo 2005)
    - Hausman test: compare OLS vs IV (significant -> endogeneity present)
    - Overidentification test (Sargan/Hansen J-test if overidentified)

References:
    Card, D. (1995). Using Geographic Variation in College Proximity to
        Estimate the Return to Schooling. In Aspects of Labour Market
        Behaviour: Essays in Honour of John Vanderkamp.
    Angrist, J. & Krueger, A. (1991). Does Compulsory School Attendance
        Affect Schooling and Earnings? QJE 106(4): 979-1014.
    Stock, J. & Yogo, M. (2005). Testing for Weak Instruments in Linear IV
        Regression. In Identification and Inference for Econometric Models.

Score: large OLS-IV gap suggests severe ability bias (STRESS). Weak instruments
(F<10) -> UNAVAILABLE or reduced confidence.
"""

import numpy as np

from app.layers.base import LayerBase


class ReturnsToEducation(LayerBase):
    layer_id = "l3"
    name = "Returns to Education (IV)"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        year = kwargs.get("year")

        year_clause = "AND dp.date = ?" if year else ""
        params = [country, "returns_education"]
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

        if not rows or len(rows) < 50:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        import json

        ln_wages = []
        schooling = []
        experience = []
        instruments = []  # distance to college, compulsory schooling indicator

        for row in rows:
            wage = row["value"]
            if wage is None or wage <= 0:
                continue
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            s = meta.get("years_schooling")
            x = meta.get("experience")
            z1 = meta.get("distance_college")
            z2 = meta.get("compulsory_schooling")
            if s is None or x is None:
                continue
            if z1 is None and z2 is None:
                continue

            ln_wages.append(np.log(wage))
            schooling.append(float(s))
            experience.append(float(x))
            z_row = []
            if z1 is not None:
                z_row.append(float(z1))
            else:
                z_row.append(0.0)
            if z2 is not None:
                z_row.append(float(z2))
            else:
                z_row.append(0.0)
            instruments.append(z_row)

        n = len(ln_wages)
        if n < 50:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid obs"}

        y = np.array(ln_wages)
        S = np.array(schooling)
        X_exog = np.column_stack([np.ones(n), np.array(experience)])
        Z = np.array(instruments)

        # OLS for comparison
        X_ols = np.column_stack([np.ones(n), S, np.array(experience)])
        beta_ols = np.linalg.lstsq(X_ols, y, rcond=None)[0]
        ols_return = beta_ols[1]

        # First stage: regress schooling on instruments + exogenous vars
        W_first = np.column_stack([X_exog, Z])
        pi = np.linalg.lstsq(W_first, S, rcond=None)[0]
        S_hat = W_first @ pi
        resid_first = S - S_hat

        # First-stage F-statistic for excluded instruments
        n_inst = Z.shape[1]
        k_first = W_first.shape[1]
        ss_res_first = np.sum(resid_first ** 2)
        # Restricted model: only exogenous regressors
        pi_r = np.linalg.lstsq(X_exog, S, rcond=None)[0]
        ss_res_restricted = np.sum((S - X_exog @ pi_r) ** 2)
        f_stat = ((ss_res_restricted - ss_res_first) / n_inst) / (ss_res_first / (n - k_first))

        # Second stage: use predicted schooling
        X_2sls = np.column_stack([np.ones(n), S_hat, np.array(experience)])
        beta_2sls = np.linalg.lstsq(X_2sls, y, rcond=None)[0]
        iv_return = beta_2sls[1]

        # Correct 2SLS standard errors (use original S, not S_hat)
        resid_2sls = y - np.column_stack([np.ones(n), S, np.array(experience)]) @ beta_2sls
        sigma2 = np.sum(resid_2sls ** 2) / (n - X_2sls.shape[1])
        XtX_hat_inv = np.linalg.inv(X_2sls.T @ X_2sls)
        se_2sls = np.sqrt(np.diag(sigma2 * XtX_hat_inv))

        # Hausman test: H = (beta_iv - beta_ols)' * (V_iv - V_ols)^(-1) * (beta_iv - beta_ols)
        resid_ols = y - X_ols @ beta_ols
        sigma2_ols = np.sum(resid_ols ** 2) / (n - X_ols.shape[1])
        V_ols = sigma2_ols * np.linalg.inv(X_ols.T @ X_ols)
        V_iv = sigma2 * XtX_hat_inv
        diff = beta_2sls - beta_ols
        V_diff = V_iv - V_ols
        # Use Moore-Penrose inverse for robustness
        hausman_stat = float(diff @ np.linalg.pinv(V_diff) @ diff)

        # Score: ability bias magnitude + instrument strength
        bias_magnitude = abs(iv_return - ols_return)
        if f_stat < 10:
            # Weak instruments: less reliable, push toward uncertain
            score = 60.0
        elif bias_magnitude > 0.05:
            score = 50.0 + bias_magnitude * 200.0
        else:
            score = 20.0 + bias_magnitude * 400.0
        score = max(0.0, min(100.0, score))

        coef_names = ["constant", "schooling", "experience"]

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "ols": {
                "return_to_schooling": round(ols_return, 4),
                "coefficients": dict(zip(coef_names, beta_ols.tolist())),
            },
            "iv_2sls": {
                "return_to_schooling": round(iv_return, 4),
                "coefficients": dict(zip(coef_names, beta_2sls.tolist())),
                "std_errors": dict(zip(coef_names, se_2sls.tolist())),
            },
            "diagnostics": {
                "first_stage_f": round(f_stat, 2),
                "weak_instrument": f_stat < 10,
                "hausman_statistic": round(hausman_stat, 4),
                "ols_iv_gap": round(iv_return - ols_return, 4),
                "ability_bias_direction": "upward" if ols_return > iv_return else "downward",
            },
        }
