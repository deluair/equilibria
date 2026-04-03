"""Education economics: production functions, class size, teacher quality, school choice.

Implements four core education economics models:

1. Education production function (Coleman 1966, Hanushek 1986):
   Maps inputs (spending, class size, teacher quality, family background)
   to outputs (test scores, graduation rates).
       Score = f(spending, class_size, teacher_quality, ses)
   Estimated via OLS with school-level data.

2. Class size effects (Angrist & Lavy 1999): exploits Maimonides' rule
   (maximum class size of 40) as an instrument for class size.
   The rule creates sharp discontinuities at enrollment thresholds:
       predicted_class_size = enrollment / ceil(enrollment / max_class)
   2SLS: instrument actual class size with Maimonides rule prediction.

3. Teacher quality value-added (Chetty, Friedman & Rockoff 2014):
   Teacher VA = residual from:
       score_it = X_it*beta + alpha_j(i,t) + e_it
   where alpha_j is teacher j's value-added, controlling for student
   characteristics X. High-VA teachers produce lasting gains in student
   outcomes (earnings, college attendance).

4. School choice and competition: Hoxby (2000) approach, testing whether
   inter-district competition (Tiebout choice) improves outcomes.
   More districts per metro area = more competition = better outcomes.
       outcome = beta0 + beta1 * n_districts + controls + epsilon

References:
    Angrist, J. & Lavy, V. (1999). Using Maimonides' Rule to Estimate the
        Effect of Class Size on Scholastic Achievement. QJE, 114(2), 533-575.
    Chetty, R., Friedman, J. & Rockoff, J. (2014). Measuring the Impacts of
        Teachers. AER, 104(9), 2633-2679.
    Hanushek, E. (2003). The Failure of Input-Based Schooling Policies.
        Economic Journal, 113(485), F64-F98.
    Hoxby, C. (2000). Does Competition Among Public Schools Benefit Students
        and Taxpayers? AER, 90(5), 1209-1238.

Sources: WDI (education indicators), UNESCO UIS
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


def _maimonides_predicted(enrollment: np.ndarray, max_class: int = 40) -> np.ndarray:
    """Maimonides rule: predicted class size = enrollment / ceil(enrollment / max_class)."""
    segments = np.ceil(enrollment / max_class)
    return enrollment / segments


class EducationEconomics(LayerBase):
    layer_id = "l10"
    name = "Education Economics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        results = {"country": country}

        # --- Education production function ---
        edu_rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN (
                'SE.XPD.TOTL.GD.ZS',
                'SE.PRM.ENRL.TC.ZS',
                'SE.SEC.CMPT.LO.ZS',
                'SE.ADT.LITR.ZS',
                'EDU_SPENDING_GDP',
                'PUPIL_TEACHER_RATIO',
                'COMPLETION_RATE',
                'LITERACY_RATE'
              )
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        # Parse latest values by series
        indicators: dict[str, float] = {}
        for r in edu_rows:
            sid = r["series_id"]
            if sid not in indicators:
                indicators[sid] = float(r["value"])

        spending = indicators.get("SE.XPD.TOTL.GD.ZS", indicators.get("EDU_SPENDING_GDP"))
        ptr = indicators.get("SE.PRM.ENRL.TC.ZS", indicators.get("PUPIL_TEACHER_RATIO"))
        completion = indicators.get("SE.SEC.CMPT.LO.ZS", indicators.get("COMPLETION_RATE"))
        literacy = indicators.get("SE.ADT.LITR.ZS", indicators.get("LITERACY_RATE"))

        # Cross-country production function estimation
        cross_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, ds.series_id, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id IN (
                'SE.XPD.TOTL.GD.ZS', 'SE.PRM.ENRL.TC.ZS',
                'SE.SEC.CMPT.LO.ZS', 'SE.ADT.LITR.ZS'
            )
              AND dp.value > 0
            ORDER BY dp.date DESC
            """
        )

        country_data: dict[str, dict[str, float]] = {}
        for r in cross_rows:
            iso = r["country_iso3"]
            sid = r["series_id"]
            if iso not in country_data:
                country_data[iso] = {}
            if sid not in country_data[iso]:
                country_data[iso][sid] = float(r["value"])

        production = {}
        # Need at least spending + PTR + outcome for estimation
        usable = {
            iso: d
            for iso, d in country_data.items()
            if "SE.XPD.TOTL.GD.ZS" in d and "SE.PRM.ENRL.TC.ZS" in d and ("SE.SEC.CMPT.LO.ZS" in d or "SE.ADT.LITR.ZS" in d)
        }

        if len(usable) >= 20:
            isos = sorted(usable.keys())
            X_spend = np.array([usable[c]["SE.XPD.TOTL.GD.ZS"] for c in isos])
            X_ptr = np.array([usable[c]["SE.PRM.ENRL.TC.ZS"] for c in isos])
            Y = np.array([usable[c].get("SE.SEC.CMPT.LO.ZS", usable[c].get("SE.ADT.LITR.ZS", 50)) for c in isos])

            # OLS: outcome = b0 + b1*spending + b2*(1/PTR) + e
            n = len(isos)
            inv_ptr = 1.0 / np.maximum(X_ptr, 1.0)
            X = np.column_stack([np.ones(n), X_spend, inv_ptr])
            beta = np.linalg.lstsq(X, Y, rcond=None)[0]
            resid = Y - X @ beta
            ss_res = float(np.sum(resid**2))
            ss_tot = float(np.sum((Y - np.mean(Y)) ** 2))
            r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0

            production = {
                "n_countries": n,
                "coefficients": {
                    "constant": round(float(beta[0]), 3),
                    "spending_pct_gdp": round(float(beta[1]), 3),
                    "inverse_ptr": round(float(beta[2]), 3),
                },
                "r_squared": round(r2, 4),
                "interpretation": {
                    "spending_effect": f"{beta[1]:.2f} pp completion per 1pp GDP spending",
                    "class_size_effect": "smaller classes improve outcomes" if beta[2] > 0 else "ambiguous",
                },
            }

            # Country-specific prediction and residual
            if country in usable:
                x_c = np.array([1.0, usable[country]["SE.XPD.TOTL.GD.ZS"], 1.0 / max(usable[country]["SE.PRM.ENRL.TC.ZS"], 1)])
                predicted = float(x_c @ beta)
                actual = usable[country].get("SE.SEC.CMPT.LO.ZS", usable[country].get("SE.ADT.LITR.ZS", 50))
                production["country_analysis"] = {
                    "predicted_outcome": round(predicted, 2),
                    "actual_outcome": round(actual, 2),
                    "residual": round(actual - predicted, 2),
                    "above_expected": actual > predicted,
                }
        else:
            production = {"error": "insufficient cross-country education data"}

        results["production_function"] = production

        # --- Angrist-Lavy class size effect ---
        # Simulate Maimonides rule analysis with enrollment data
        school_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id LIKE '%SCHOOL_ENROLLMENT%'
            """,
            (country,),
        )

        if school_rows and len(school_rows) >= 20:
            import json

            enrollments = []
            actual_sizes = []
            scores = []
            for r in school_rows:
                meta = json.loads(r["metadata"]) if r.get("metadata") else {}
                enr = float(r["value"])
                act = meta.get("avg_class_size")
                sc = meta.get("avg_score")
                if act is not None and sc is not None and enr > 0:
                    enrollments.append(enr)
                    actual_sizes.append(float(act))
                    scores.append(float(sc))

            if len(enrollments) >= 20:
                enr_arr = np.array(enrollments)
                act_arr = np.array(actual_sizes)
                sc_arr = np.array(scores)
                predicted_sizes = _maimonides_predicted(enr_arr)

                # First stage: actual_size = a0 + a1*predicted_size + e
                X_fs = np.column_stack([np.ones(len(enr_arr)), predicted_sizes])
                a = np.linalg.lstsq(X_fs, act_arr, rcond=None)[0]
                fitted_sizes = X_fs @ a
                fs_r2 = 1.0 - np.sum((act_arr - fitted_sizes) ** 2) / np.sum((act_arr - np.mean(act_arr)) ** 2)

                # Second stage: score = b0 + b1*fitted_size + e
                X_ss = np.column_stack([np.ones(len(fitted_sizes)), fitted_sizes])
                b = np.linalg.lstsq(X_ss, sc_arr, rcond=None)[0]
                # OLS for comparison (biased)
                X_ols = np.column_stack([np.ones(len(act_arr)), act_arr])
                b_ols = np.linalg.lstsq(X_ols, sc_arr, rcond=None)[0]

                results["class_size_angrist_lavy"] = {
                    "n_schools": len(enrollments),
                    "first_stage_f": round(float(a[1]) ** 2 * np.var(predicted_sizes) / np.var(act_arr - fitted_sizes), 2),
                    "first_stage_r2": round(float(fs_r2), 4),
                    "iv_class_size_effect": round(float(b[1]), 3),
                    "ols_class_size_effect": round(float(b_ols[1]), 3),
                    "iv_stronger": abs(b[1]) > abs(b_ols[1]),
                }
            else:
                results["class_size_angrist_lavy"] = {"error": "insufficient school-level data"}
        else:
            results["class_size_angrist_lavy"] = {"error": "no school enrollment microdata"}

        # --- Teacher quality value-added ---
        # Cross-country proxy: correlate teacher qualifications with outcomes
        teacher_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id IN ('SE.PRM.TCAQ.ZS', 'TEACHER_QUALIFIED_PCT')
              AND dp.value > 0
            ORDER BY dp.date DESC
            """
        )

        outcome_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id IN ('SE.SEC.CMPT.LO.ZS', 'COMPLETION_RATE')
              AND dp.value > 0
            ORDER BY dp.date DESC
            """
        )

        if teacher_rows and outcome_rows:
            tq_latest: dict[str, float] = {}
            for r in teacher_rows:
                iso = r["country_iso3"]
                if iso not in tq_latest:
                    tq_latest[iso] = float(r["value"])

            out_latest: dict[str, float] = {}
            for r in outcome_rows:
                iso = r["country_iso3"]
                if iso not in out_latest:
                    out_latest[iso] = float(r["value"])

            common_iso = sorted(set(tq_latest) & set(out_latest))
            if len(common_iso) >= 15:
                x = np.array([tq_latest[c] for c in common_iso])
                y = np.array([out_latest[c] for c in common_iso])

                slope, intercept, r_value, p_value, std_err = sp_stats.linregress(x, y)

                results["teacher_value_added"] = {
                    "n_countries": len(common_iso),
                    "slope": round(float(slope), 4),
                    "r_squared": round(float(r_value**2), 4),
                    "p_value": round(float(p_value), 4),
                    "interpretation": (f"1pp increase in qualified teachers -> {slope:.2f}pp completion"),
                    "country_teacher_quality": round(tq_latest[country], 1) if country in tq_latest else None,
                }
            else:
                results["teacher_value_added"] = {"error": "insufficient teacher quality data"}
        else:
            results["teacher_value_added"] = {"error": "no teacher qualification data"}

        # --- School choice and competition ---
        # Cross-country: private school share as proxy for competition
        private_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id IN ('SE.PRM.PRIV.ZS', 'PRIVATE_SCHOOL_SHARE')
              AND dp.value >= 0
            ORDER BY dp.date DESC
            """
        )

        if private_rows and outcome_rows:
            priv_latest: dict[str, float] = {}
            for r in private_rows:
                iso = r["country_iso3"]
                if iso not in priv_latest:
                    priv_latest[iso] = float(r["value"])

            common_iso2 = sorted(set(priv_latest) & set(out_latest))
            if len(common_iso2) >= 15:
                x2 = np.array([priv_latest[c] for c in common_iso2])
                y2 = np.array([out_latest[c] for c in common_iso2])

                slope2, _, r_val2, p_val2, _ = sp_stats.linregress(x2, y2)

                results["school_choice"] = {
                    "n_countries": len(common_iso2),
                    "private_share_effect": round(float(slope2), 4),
                    "r_squared": round(float(r_val2**2), 4),
                    "p_value": round(float(p_val2), 4),
                    "country_private_share": round(priv_latest[country], 1) if country in priv_latest else None,
                    "competition_matters": slope2 > 0 and p_val2 < 0.1,
                }
            else:
                results["school_choice"] = {"error": "insufficient private school data"}
        else:
            results["school_choice"] = {"error": "no private school enrollment data"}

        # --- Country indicators summary ---
        results["indicators"] = {
            "spending_pct_gdp": round(spending, 2) if spending else None,
            "pupil_teacher_ratio": round(ptr, 1) if ptr else None,
            "completion_rate": round(completion, 1) if completion else None,
            "literacy_rate": round(literacy, 1) if literacy else None,
        }

        # --- Score ---
        score = 25.0

        # High PTR (class size proxy)
        if ptr and ptr > 40:
            score += 25
        elif ptr and ptr > 25:
            score += 10

        # Low spending
        if spending and spending < 3.0:
            score += 15
        elif spending and spending < 4.0:
            score += 5

        # Low completion
        if completion and completion < 50:
            score += 20
        elif completion and completion < 75:
            score += 10

        # Below-expected performance
        pf = results.get("production_function", {})
        ca = pf.get("country_analysis", {})
        if ca.get("residual") is not None and ca["residual"] < -10:
            score += 10

        score = max(0.0, min(100.0, score))

        return {"score": round(score, 1), "results": results}
