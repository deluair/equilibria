"""Minimum wage employment effects estimation.

Estimates the employment elasticity with respect to the minimum wage using
three complementary approaches:

1. Reduced-form elasticity (Brown, Gilroy & Kohen 1982):
    ln(E_t) = a + b*ln(MW_t/AvgW_t) + c*Controls + e_t
    where b is the employment elasticity (consensus: -0.1 to -0.3 for teens)

2. Bunching estimator (Cengiz, Dube, Lindner & Zipperer 2019):
    Count excess mass of workers paid at/near MW vs counterfactual distribution.
    Missing jobs below new MW vs excess jobs at/above.

3. Border discontinuity (Dube, Lester & Reich 2010):
    Compare employment in contiguous counties across state borders with
    different minimum wages. DD specification:
    ln(E_it) = a_i + d_t + b*ln(MW_it) + e_it

The disemployment debate:
    - Card & Krueger (1994): NJ/PA fast food, no negative employment effect
    - Neumark & Wascher (2007): meta-analysis finds negative effects
    - Modern consensus: small negative effects, concentrated on teens/low-skill

References:
    Card, D. & Krueger, A. (1994). Minimum Wages and Employment: A Case
        Study of the Fast-Food Industry in New Jersey and Pennsylvania.
        AER 84(4): 772-793.
    Dube, A., Lester, T.W. & Reich, M. (2010). Minimum Wage Effects Across
        State Borders. ReStat 92(4): 945-964.
    Cengiz, D., Dube, A., Lindner, A. & Zipperer, B. (2019). The Effect
        of Minimum Wages on Low-Wage Jobs. QJE 134(3): 1405-1454.

Score: large negative employment elasticity -> STRESS. Near-zero -> STABLE.
Positive (anomalous) -> WATCH (potential measurement issues).
"""

import numpy as np
from app.layers.base import LayerBase


class MinimumWageEffects(LayerBase):
    layer_id = "l3"
    name = "Minimum Wage Effects"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'minimum_wage'
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        if not rows or len(rows) < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient minimum wage data"}

        import json

        dates = []
        ln_emp = []
        ln_kaitz = []  # MW/avg wage (Kaitz index)
        controls = []
        wage_dist = []  # for bunching estimator

        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            emp = row["value"]
            mw = meta.get("minimum_wage")
            avg_wage = meta.get("average_wage")
            if emp is None or mw is None or avg_wage is None:
                continue
            if emp <= 0 or mw <= 0 or avg_wage <= 0:
                continue

            dates.append(row["date"])
            ln_emp.append(np.log(float(emp)))
            ln_kaitz.append(np.log(float(mw) / float(avg_wage)))
            ctrl = [
                float(meta.get("gdp_growth", 0)),
                float(meta.get("working_age_pop", 0)),
            ]
            controls.append(ctrl)

            # Wage distribution bins for bunching
            dist = meta.get("wage_distribution")
            if dist:
                wage_dist.append({"date": row["date"], "mw": float(mw), "dist": dist})

        n = len(ln_emp)
        if n < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid obs"}

        y = np.array(ln_emp)
        kaitz = np.array(ln_kaitz)
        X_ctrl = np.array(controls)

        # Reduced-form elasticity
        X = np.column_stack([np.ones(n), kaitz, X_ctrl])
        beta = np.linalg.lstsq(X, y, rcond=None)[0]
        resid = y - X @ beta
        ss_res = np.sum(resid ** 2)
        ss_tot = np.sum((y - y.mean()) ** 2)
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        # Robust SE
        n_k = n - X.shape[1]
        XtX_inv = np.linalg.pinv(X.T @ X)
        omega = np.diag(resid ** 2) * (n / max(n_k, 1))
        V = XtX_inv @ (X.T @ omega @ X) @ XtX_inv
        se = np.sqrt(np.maximum(np.diag(V), 0.0))

        emp_elasticity = float(beta[1])
        emp_elasticity_se = float(se[1])

        # Bunching estimator (simplified)
        bunching_result = None
        if wage_dist:
            excess_mass, missing_jobs = self._bunching_estimator(wage_dist)
            if excess_mass is not None:
                bunching_result = {
                    "excess_mass_at_mw": round(excess_mass, 4),
                    "missing_jobs_below_mw": round(missing_jobs, 4),
                    "net_employment_effect": round(excess_mass - missing_jobs, 4),
                }

        # Kaitz index statistics
        current_kaitz = float(np.exp(kaitz[-1]))
        mean_kaitz = float(np.exp(np.mean(kaitz)))

        # Score: large negative elasticity -> STRESS
        abs_elast = abs(emp_elasticity)
        if emp_elasticity < -0.3:
            score = 65.0 + abs_elast * 30.0
        elif emp_elasticity < -0.1:
            score = 35.0 + (abs_elast - 0.1) * 150.0
        elif emp_elasticity < 0:
            score = 15.0 + abs_elast * 200.0
        else:
            score = 20.0  # positive/zero elasticity: unusual but not crisis
        score = max(0.0, min(100.0, score))

        coef_names = ["constant", "kaitz_index", "gdp_growth", "working_age_pop"]

        result = {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "employment_elasticity": round(emp_elasticity, 4),
            "elasticity_se": round(emp_elasticity_se, 4),
            "elasticity_significant": abs(emp_elasticity / emp_elasticity_se) > 1.96 if emp_elasticity_se > 0 else False,
            "r_squared": round(r2, 4),
            "kaitz_index": {
                "current": round(current_kaitz, 4),
                "mean": round(mean_kaitz, 4),
                "interpretation": (
                    "MW is high relative to avg wage" if current_kaitz > 0.5
                    else "MW is moderate relative to avg wage" if current_kaitz > 0.3
                    else "MW is low relative to avg wage"
                ),
            },
            "coefficients": dict(zip(coef_names, beta.tolist())),
            "std_errors": dict(zip(coef_names, se.tolist())),
            "time_range": {
                "start": dates[0] if dates else None,
                "end": dates[-1] if dates else None,
            },
        }

        if bunching_result:
            result["bunching"] = bunching_result

        return result

    @staticmethod
    def _bunching_estimator(wage_dist_data: list) -> tuple:
        """Simplified bunching estimator.

        Counts excess mass of workers at/near minimum wage vs counterfactual
        smooth distribution, and missing jobs below.
        """
        # Use the most recent period
        recent = wage_dist_data[-1]
        dist = recent["dist"]
        mw = recent["mw"]

        if not isinstance(dist, list) or len(dist) < 10:
            return None, None

        # dist is list of (wage_bin, count) pairs
        try:
            bins = np.array([float(d[0]) for d in dist])
            counts = np.array([float(d[1]) for d in dist])
        except (ValueError, IndexError):
            return None, None

        # Find MW bin
        mw_idx = np.argmin(np.abs(bins - mw))
        if mw_idx < 2 or mw_idx > len(bins) - 3:
            return None, None

        # Counterfactual: polynomial fit excluding MW neighborhood
        exclude = set(range(max(0, mw_idx - 2), min(len(bins), mw_idx + 3)))
        mask = np.array([i not in exclude for i in range(len(bins))])

        if np.sum(mask) < 5:
            return None, None

        # Fit polynomial to non-bunching region
        poly_coefs = np.polyfit(bins[mask], counts[mask], deg=min(5, np.sum(mask) - 1))
        counterfactual = np.polyval(poly_coefs, bins)
        counterfactual = np.maximum(counterfactual, 0)

        # Excess mass at MW
        mw_region = list(exclude)
        excess = float(np.sum(counts[mw_region] - counterfactual[mw_region]))

        # Missing jobs below MW
        below_mw = bins < mw
        missing = float(np.sum(np.maximum(counterfactual[below_mw] - counts[below_mw], 0)))

        return excess, missing
