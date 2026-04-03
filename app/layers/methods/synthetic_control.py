"""Synthetic Control Method (Abadie, Diamond & Hainmueller 2010).

Constructs a synthetic counterfactual for a treated unit by finding a weighted
combination of donor (untreated) units that best reproduces the treated unit's
pre-treatment trajectory. The treatment effect is the gap between the treated
unit and its synthetic counterpart in the post-treatment period.

Key steps:
    1. Donor pool selection: exclude units affected by spillovers
    2. Pre-treatment fit: minimize RMSPE via constrained optimization
       (donor weights >= 0, sum to 1)
    3. Treatment effect: Y_treated - Y_synthetic in post-treatment
    4. Placebo inference: re-run SCM treating each donor as if treated,
       compute p-value from ratio of post/pre RMSPE
    5. Leave-one-out: drop each donor to assess sensitivity

The optimization solves:
    min_W ||X1 - X0 * W||_V
    s.t. w_j >= 0, sum(w_j) = 1

where V is a diagonal matrix weighting predictor importance (selected by
cross-validation or nested optimization).

References:
    Abadie, A. & Gardeazabal, J. (2003). The Economic Costs of Conflict:
        A Case Study of the Basque Country. AER 93(1): 113-132.
    Abadie, A., Diamond, A. & Hainmueller, J. (2010). Synthetic Control
        Methods for Comparative Case Studies. JASA 105(490): 493-505.
    Abadie, A., Diamond, A. & Hainmueller, J. (2015). Comparative Politics
        and the Synthetic Control Method. AJPS 59(2): 495-510.

Score: large post-treatment gap (poor synthetic fit or large effect) -> high
score (STRESS). Good pre-treatment fit with small gap -> STABLE.
"""

import json

import numpy as np
from scipy.optimize import minimize

from app.layers.base import LayerBase


class SyntheticControl(LayerBase):
    layer_id = "l18"
    name = "Synthetic Control Method"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        treatment_year = kwargs.get("treatment_year")
        outcome_var = kwargs.get("outcome_var", "gdp_pc")

        rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date, ds.metadata, ds.country_iso3
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'synthetic_control'
              AND ds.description LIKE ?
            ORDER BY ds.country_iso3, dp.date
            """,
            (f"%{outcome_var}%",),
        )

        if not rows or len(rows) < 20:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient panel data"}

        # Parse into country-year panels
        panels = {}
        for row in rows:
            iso3 = row["country_iso3"]
            date = row["date"]
            val = row["value"]
            if val is None:
                continue
            panels.setdefault(iso3, {})[date] = float(val)

        if country not in panels:
            return {"score": None, "signal": "UNAVAILABLE", "error": f"no data for {country}"}

        # Determine treatment year from metadata if not provided
        if treatment_year is None:
            for row in rows:
                if row["country_iso3"] == country:
                    meta = json.loads(row["metadata"]) if row.get("metadata") else {}
                    if meta.get("treatment_year"):
                        treatment_year = int(meta["treatment_year"])
                        break

        if treatment_year is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "treatment_year required"}

        # Build aligned matrix: all periods common across treated + donors
        treated_dates = sorted(panels[country].keys())
        donors = {k: v for k, v in panels.items() if k != country}

        if len(donors) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "need >= 2 donor units"}

        # Common dates
        common_dates = set(treated_dates)
        for d_data in donors.values():
            common_dates &= set(d_data.keys())
        common_dates = sorted(common_dates)

        pre_dates = [d for d in common_dates if d < str(treatment_year)]
        post_dates = [d for d in common_dates if d >= str(treatment_year)]

        if len(pre_dates) < 3 or len(post_dates) < 1:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient pre/post periods"}

        # Treated vector
        y1_pre = np.array([panels[country][d] for d in pre_dates])
        y1_post = np.array([panels[country][d] for d in post_dates])

        # Donor matrix
        donor_names = sorted(donors.keys())
        J = len(donor_names)
        Y0_pre = np.column_stack([
            [donors[dn][d] for d in pre_dates] for dn in donor_names
        ])
        Y0_post = np.column_stack([
            [donors[dn][d] for d in post_dates] for dn in donor_names
        ])

        # Solve for optimal weights: min ||y1_pre - Y0_pre @ w||^2
        # subject to w >= 0, sum(w) = 1
        w_opt = self._solve_weights(y1_pre, Y0_pre)

        # Synthetic counterfactual
        y_synth_pre = Y0_pre @ w_opt
        y_synth_post = Y0_post @ w_opt

        # Pre-treatment fit (RMSPE)
        pre_rmspe = float(np.sqrt(np.mean((y1_pre - y_synth_pre) ** 2)))

        # Post-treatment gap
        gaps_post = y1_post - y_synth_post
        post_rmspe = float(np.sqrt(np.mean(gaps_post ** 2)))
        att = float(np.mean(gaps_post))

        # Ratio of post/pre RMSPE (used for inference)
        rmspe_ratio = post_rmspe / pre_rmspe if pre_rmspe > 1e-10 else float("inf")

        # Placebo permutation inference
        placebo_ratios = []
        for j, dn in enumerate(donor_names):
            # Treat donor j as "treated"
            y1_plac = Y0_pre[:, j]
            Y0_plac = np.delete(Y0_pre, j, axis=1)
            if Y0_plac.shape[1] < 1:
                continue
            w_plac = self._solve_weights(y1_plac, Y0_plac)
            synth_plac_pre = Y0_plac @ w_plac

            y1_plac_post = Y0_post[:, j]
            Y0_plac_post = np.delete(Y0_post, j, axis=1)
            synth_plac_post = Y0_plac_post @ w_plac

            pre_rmspe_plac = float(np.sqrt(np.mean((y1_plac - synth_plac_pre) ** 2)))
            post_rmspe_plac = float(np.sqrt(np.mean((y1_plac_post - synth_plac_post) ** 2)))
            if pre_rmspe_plac > 1e-10:
                placebo_ratios.append(post_rmspe_plac / pre_rmspe_plac)

        # P-value: fraction of placebo ratios >= treated ratio
        p_value = None
        if placebo_ratios:
            p_value = float(np.mean(np.array(placebo_ratios) >= rmspe_ratio))

        # Leave-one-out sensitivity
        loo_gaps = []
        for j in range(J):
            Y0_loo = np.delete(Y0_pre, j, axis=1)
            Y0_loo_post = np.delete(Y0_post, j, axis=1)
            if Y0_loo.shape[1] < 1:
                continue
            w_loo = self._solve_weights(y1_pre, Y0_loo)
            synth_loo_post = Y0_loo_post @ w_loo
            loo_gaps.append(float(np.mean(y1_post - synth_loo_post)))

        loo_range = (min(loo_gaps), max(loo_gaps)) if loo_gaps else (None, None)

        # Donor weights (nonzero only)
        donor_weights = {
            dn: round(float(w_opt[j]), 4)
            for j, dn in enumerate(donor_names)
            if w_opt[j] > 0.001
        }

        # Score: rmspe_ratio captures how unusual the post-treatment gap is
        # Higher ratio -> more unusual -> higher score
        if rmspe_ratio > 10:
            score = 85.0
        elif rmspe_ratio > 5:
            score = 60.0 + (rmspe_ratio - 5) * 5.0
        elif rmspe_ratio > 2:
            score = 30.0 + (rmspe_ratio - 2) * 10.0
        else:
            score = rmspe_ratio * 15.0
        score = max(0.0, min(100.0, score))

        return {
            "score": round(score, 2),
            "country": country,
            "treatment_year": treatment_year,
            "outcome_var": outcome_var,
            "n_donors": J,
            "n_pre_periods": len(pre_dates),
            "n_post_periods": len(post_dates),
            "pre_rmspe": round(pre_rmspe, 4),
            "post_rmspe": round(post_rmspe, 4),
            "rmspe_ratio": round(rmspe_ratio, 4),
            "att": round(att, 4),
            "p_value": round(p_value, 4) if p_value is not None else None,
            "donor_weights": donor_weights,
            "post_treatment_gaps": [round(float(g), 4) for g in gaps_post],
            "leave_one_out": {
                "att_range": [round(v, 4) if v is not None else None for v in loo_range],
            },
        }

    @staticmethod
    def _solve_weights(y1: np.ndarray, Y0: np.ndarray) -> np.ndarray:
        """Solve for donor weights minimizing pre-treatment MSPE.

        min_w ||y1 - Y0 @ w||^2  s.t. w >= 0, sum(w) = 1
        """
        J = Y0.shape[1]
        if J == 1:
            return np.array([1.0])

        def objective(w):
            return float(np.sum((y1 - Y0 @ w) ** 2))

        constraints = {"type": "eq", "fun": lambda w: np.sum(w) - 1.0}
        bounds = [(0.0, 1.0)] * J
        w0 = np.ones(J) / J

        result = minimize(objective, w0, method="SLSQP", bounds=bounds,
                          constraints=constraints, options={"maxiter": 1000, "ftol": 1e-12})
        return result.x
