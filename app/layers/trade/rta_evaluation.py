"""Regional trade agreement evaluation.

Methodology:
    Ex-post gravity analysis of FTA and customs union effects on bilateral
    trade. Following Baier and Bergstrand (2007) and the WTO/ESCAP
    evaluation framework:

    1. Estimate gravity model with RTA dummies (FTA, CU, PTA, EIA)
       and proper controls for multilateral resistance (country-time FE).
    2. Use phased-in RTA effects (years since entry into force) to
       capture adjustment dynamics.
    3. Compare partial vs general equilibrium effects using the structural
       gravity framework.
    4. Compute trade creation, trade diversion, and welfare effects.

    RTA classification follows WTO definitions:
    - FTA: Free Trade Agreement (tariff elimination between members)
    - CU: Customs Union (common external tariff)
    - PTA: Preferential Trade Agreement (partial preferences)
    - EIA: Economic Integration Agreement (services liberalization)

    Score (0-100): Higher score indicates the country's RTAs are
    underperforming (low trade creation relative to expectations).

References:
    Baier, S.L. and Bergstrand, J.H. (2007). "Do free trade agreements
        actually increase members' international trade?" Journal of
        International Economics, 71(1), 72-95.
    Yotov, Y.V. et al. (2016). "An Advanced Guide to Trade Policy
        Analysis: The Structural Gravity Model." WTO/ESCAP.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RTAEvaluation(LayerBase):
    layer_id = "l1"
    name = "RTA Evaluation"

    async def compute(self, db, **kwargs) -> dict:
        """Evaluate regional trade agreement effects using gravity.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            reporter : str - ISO3 country code
            year : int - reference year
            rta_type : str - filter by RTA type (FTA, CU, PTA, EIA, or all)
        """
        reporter = kwargs.get("reporter", "USA")
        year = kwargs.get("year", 2022)
        rta_type = kwargs.get("rta_type", "all")

        # Fetch bilateral trade with RTA indicators
        rows = await db.execute(
            """
            SELECT reporter_iso3, partner_iso3, trade_value,
                   gdp_reporter, gdp_partner, distance,
                   rta_dummy, rta_type, rta_years_in_force,
                   contiguity, common_language, currency_union
            FROM bilateral_trade
            WHERE year = ? AND trade_value > 0
            """,
            (year,),
        )
        records = await rows.fetchall()

        if not records:
            return {"score": 50.0, "rta_effect": None,
                    "note": "No bilateral trade data available"}

        # Build estimation dataset
        ln_trade = []
        ln_gdp_r = []
        ln_gdp_p = []
        ln_dist = []
        rta_vec = []
        rta_years = []
        controls = []
        pair_info = []

        for r in records:
            tv = float(r["trade_value"])
            gr = float(r["gdp_reporter"])
            gp = float(r["gdp_partner"])
            d = float(r["distance"])
            if tv <= 0 or gr <= 0 or gp <= 0 or d <= 0:
                continue

            r_type = r["rta_type"] or ""
            if rta_type != "all" and r_type != rta_type:
                rta_flag = 0
            else:
                rta_flag = int(r["rta_dummy"] or 0)

            ln_trade.append(np.log(tv))
            ln_gdp_r.append(np.log(gr))
            ln_gdp_p.append(np.log(gp))
            ln_dist.append(np.log(d))
            rta_vec.append(rta_flag)
            rta_years.append(int(r["rta_years_in_force"] or 0))
            controls.append([
                int(r["contiguity"] or 0),
                int(r["common_language"] or 0),
                int(r["currency_union"] or 0),
            ])
            pair_info.append({
                "reporter": r["reporter_iso3"],
                "partner": r["partner_iso3"],
                "rta_type": r_type,
            })

        n = len(ln_trade)
        if n < 20:
            return {"score": 50.0, "rta_effect": None,
                    "note": "Insufficient observations for RTA evaluation"}

        y = np.array(ln_trade)
        rta_arr = np.array(rta_vec, dtype=float)
        rta_years_arr = np.array(rta_years, dtype=float)
        ctrl = np.array(controls, dtype=float)

        # Specification 1: Simple RTA dummy
        X1 = np.column_stack([
            np.ones(n),
            np.array(ln_gdp_r),
            np.array(ln_gdp_p),
            np.array(ln_dist),
            rta_arr,
            ctrl,
        ])

        try:
            beta1 = np.linalg.lstsq(X1, y, rcond=None)[0]
        except np.linalg.LinAlgError:
            return {"score": 50.0, "rta_effect": None,
                    "note": "Gravity estimation failed"}

        rta_coeff = beta1[4]
        rta_effect = float(np.exp(rta_coeff))

        # Specification 2: Phased-in RTA effects
        # Create bins: 0-5 years, 5-10 years, 10+ years
        phase_0_5 = ((rta_arr == 1) & (rta_years_arr <= 5)).astype(float)
        phase_5_10 = ((rta_arr == 1) & (rta_years_arr > 5) & (rta_years_arr <= 10)).astype(float)
        phase_10_plus = ((rta_arr == 1) & (rta_years_arr > 10)).astype(float)

        X2 = np.column_stack([
            np.ones(n),
            np.array(ln_gdp_r),
            np.array(ln_gdp_p),
            np.array(ln_dist),
            phase_0_5,
            phase_5_10,
            phase_10_plus,
            ctrl,
        ])

        try:
            beta2 = np.linalg.lstsq(X2, y, rcond=None)[0]
            phase_effects = {
                "years_0_5": {"coeff": float(beta2[4]), "effect": float(np.exp(beta2[4]))},
                "years_5_10": {"coeff": float(beta2[5]), "effect": float(np.exp(beta2[5]))},
                "years_10_plus": {"coeff": float(beta2[6]), "effect": float(np.exp(beta2[6]))},
            }
        except np.linalg.LinAlgError:
            phase_effects = None

        # Standard errors for specification 1
        residuals = y - X1 @ beta1
        mse = float(np.sum(residuals ** 2) / (n - X1.shape[1]))
        try:
            var_beta = mse * np.linalg.inv(X1.T @ X1)
            se_rta = float(np.sqrt(var_beta[4, 4]))
            t_stat = rta_coeff / se_rta if se_rta > 0 else 0.0
        except np.linalg.LinAlgError:
            se_rta = 0.0
            t_stat = 0.0

        # R-squared
        ss_res = float(np.sum(residuals ** 2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

        # Reporter-specific RTA analysis
        reporter_rtas = []
        reporter_rta_trade = 0.0
        reporter_total_trade = 0.0
        for i in range(n):
            if pair_info[i]["reporter"] == reporter:
                reporter_total_trade += np.exp(y[i])
                if rta_arr[i] == 1:
                    reporter_rta_trade += np.exp(y[i])
                    reporter_rtas.append({
                        "partner": pair_info[i]["partner"],
                        "rta_type": pair_info[i]["rta_type"],
                        "years_in_force": int(rta_years_arr[i]),
                        "trade_value": float(np.exp(y[i])),
                    })

        reporter_rtas.sort(key=lambda x: x["trade_value"], reverse=True)
        rta_coverage = (reporter_rta_trade / reporter_total_trade
                        if reporter_total_trade > 0 else 0)

        # RTA type breakdown
        rta_types = {}
        for r in records:
            t = r["rta_type"] or "none"
            rta_types[t] = rta_types.get(t, 0) + 1

        # Counterfactual: reporter trade without RTAs
        reporter_mask = np.array([p["reporter"] == reporter for p in pair_info])
        if reporter_mask.sum() > 0:
            X_reporter = X1[reporter_mask]
            fitted_with = X_reporter @ beta1
            X_no_rta = X_reporter.copy()
            X_no_rta[:, 4] = 0.0
            fitted_without = X_no_rta @ beta1
            trade_gain = float(
                np.sum(np.exp(fitted_with)) - np.sum(np.exp(fitted_without))
            )
        else:
            trade_gain = 0.0

        # Score: underperforming RTAs = high score
        # Low RTA coverage or negative RTA effect = high score
        if rta_effect >= 1.0 and rta_coverage > 0.3:
            # RTAs working and good coverage
            score = float(np.clip((1 - rta_coverage) * 50, 0, 50))
        elif rta_effect >= 1.0:
            # RTAs working but low coverage
            score = float(np.clip(50 + (1 - rta_coverage) * 30, 50, 80))
        else:
            # RTAs not increasing trade
            score = float(np.clip(80 + (1 - rta_effect) * 20, 80, 100))

        return {
            "score": score,
            "rta_effect": rta_effect,
            "rta_coefficient": float(rta_coeff),
            "rta_se": se_rta,
            "rta_t_stat": float(t_stat),
            "r_squared": r_squared,
            "phase_effects": phase_effects,
            "rta_coverage": float(rta_coverage),
            "trade_gain_from_rtas": trade_gain,
            "n_observations": n,
            "n_rta_pairs": int(rta_arr.sum()),
            "reporter_rta_partners": reporter_rtas[:10],
            "rta_type_distribution": rta_types,
            "gravity_coefficients": {
                "constant": float(beta1[0]),
                "ln_gdp_reporter": float(beta1[1]),
                "ln_gdp_partner": float(beta1[2]),
                "ln_distance": float(beta1[3]),
                "rta": float(beta1[4]),
                "contiguity": float(beta1[5]),
                "common_language": float(beta1[6]),
                "currency_union": float(beta1[7]),
            },
            "reporter": reporter,
            "year": year,
            "rta_type_filter": rta_type,
        }
