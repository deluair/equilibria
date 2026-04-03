"""Health expenditure decomposition and efficiency analysis.

Decomposes total health expenditure into public, private, and out-of-pocket
components. Estimates the Newhouse (1977) income elasticity of health spending,
tests Wagner's law for health (public share rises with income), and constructs
a cross-country efficiency frontier using Data Envelopment Analysis (DEA).

Key references:
    Newhouse, J.P. (1977). Medical care expenditure: a cross-national survey.
        Journal of Human Resources, 12(1), 115-125.
    Wagstaff, A. (2009). Social health insurance vs. tax-financed health
        systems: evidence from the OECD. Policy Research WP 4821, World Bank.
    Charnes, A., Cooper, W.W. & Rhodes, E. (1978). Measuring the efficiency
        of decision making units. EJOR, 2(6), 429-444.
"""

from __future__ import annotations

import numpy as np
from scipy import optimize

from app.layers.base import LayerBase


def _dea_output_oriented(inputs: np.ndarray, outputs: np.ndarray) -> np.ndarray:
    """Output-oriented CRS DEA efficiency scores.

    inputs: (n, m) array of m input variables for n DMUs
    outputs: (n, s) array of s output variables for n DMUs
    Returns: (n,) efficiency scores in [0, 1]. 1 = frontier.
    """
    n = inputs.shape[0]
    scores = np.ones(n)

    for i in range(n):
        # Maximize phi s.t. sum(lambda_j * y_j) >= phi * y_i,
        #                     sum(lambda_j * x_j) <= x_i,
        #                     lambda >= 0
        # Reformulate as LP minimization of -phi.
        m = inputs.shape[1]
        s = outputs.shape[1]

        # Decision variables: [lambda_1..lambda_n, phi]
        c = np.zeros(n + 1)
        c[-1] = -1.0  # minimize -phi

        # Output constraints: sum(lambda_j * y_jr) - phi * y_ir >= 0
        # => -sum(lambda_j * y_jr) + phi * y_ir <= 0
        A_ub_out = np.zeros((s, n + 1))
        for r in range(s):
            A_ub_out[r, :n] = -outputs[:, r]
            A_ub_out[r, -1] = outputs[i, r]
        b_ub_out = np.zeros(s)

        # Input constraints: sum(lambda_j * x_jm) <= x_im
        A_ub_in = np.zeros((m, n + 1))
        for j in range(m):
            A_ub_in[j, :n] = inputs[:, j]
        b_ub_in = inputs[i, :]

        A_ub = np.vstack([A_ub_out, A_ub_in])
        b_ub = np.concatenate([b_ub_out, b_ub_in])

        bounds = [(0, None)] * n + [(1, None)]  # phi >= 1

        res = optimize.linprog(c, A_ub=A_ub, b_ub=b_ub, bounds=bounds, method="highs")
        if res.success:
            phi = res.x[-1]
            scores[i] = 1.0 / phi if phi > 0 else 1.0

    return scores


class HealthExpenditure(LayerBase):
    layer_id = "l8"
    name = "Health Expenditure"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Decompose health spending and estimate efficiency.

        Fetches health expenditure indicators (total, public, OOP as % of GDP
        or current health expenditure), GDP per capita, and health outcomes
        (life expectancy). Estimates Newhouse income elasticity, tests Wagner's
        law for health, and computes DEA efficiency scores.

        Returns dict with score, expenditure decomposition, income elasticity,
        Wagner's law test, and DEA efficiency rankings.
        """
        country_iso3 = kwargs.get("country_iso3")

        # Total health expenditure as % of GDP (SH.XPD.CHEX.GD.ZS)
        the_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.XPD.CHEX.GD.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Out-of-pocket expenditure as % of current health expenditure
        oop_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.XPD.OOPC.CH.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Domestic general government health expenditure as % of CHE
        pub_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.XPD.GHED.CH.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # GDP per capita (constant USD)
        gdppc_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Life expectancy at birth
        le_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.DYN.LE00.IN'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not the_rows or not gdppc_rows:
            return {"score": 50, "results": {"error": "no health expenditure or GDP data"}}

        # Index data by country -> year
        def _index(rows):
            out: dict[str, dict[str, float]] = {}
            for r in rows:
                out.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]
            return out

        the_data = _index(the_rows)
        oop_data = _index(oop_rows) if oop_rows else {}
        pub_data = _index(pub_rows) if pub_rows else {}
        gdppc_data = _index(gdppc_rows)
        le_data = _index(le_rows) if le_rows else {}

        # --- Newhouse income elasticity ---
        # Cross-country regression: log(THE/GDP) = a + b*log(GDPpc)
        log_the, log_gdppc = [], []
        for iso in set(the_data.keys()) & set(gdppc_data.keys()):
            years = sorted(set(the_data[iso].keys()) & set(gdppc_data[iso].keys()))
            if years:
                # Use latest available year
                yr = years[-1]
                t_val = the_data[iso][yr]
                g_val = gdppc_data[iso][yr]
                if t_val and t_val > 0 and g_val and g_val > 0:
                    log_the.append(np.log(t_val))
                    log_gdppc.append(np.log(g_val))

        newhouse = None
        if len(log_the) >= 20:
            log_the_arr = np.array(log_the)
            log_gdppc_arr = np.array(log_gdppc)

            # OLS: log(THE) = alpha + beta*log(GDPpc)
            X = np.column_stack([np.ones(len(log_gdppc_arr)), log_gdppc_arr])
            beta, residuals, _, _ = np.linalg.lstsq(X, log_the_arr, rcond=None)
            y_hat = X @ beta
            ss_res = np.sum((log_the_arr - y_hat) ** 2)
            ss_tot = np.sum((log_the_arr - np.mean(log_the_arr)) ** 2)
            r_sq = 1 - ss_res / ss_tot if ss_tot > 0 else 0

            # Standard error of beta
            n_obs = len(log_the_arr)
            mse = ss_res / (n_obs - 2) if n_obs > 2 else 0
            x_var = np.sum((log_gdppc_arr - np.mean(log_gdppc_arr)) ** 2)
            se_beta = np.sqrt(mse / x_var) if x_var > 0 else 0

            newhouse = {
                "income_elasticity": float(beta[1]),
                "se": float(se_beta),
                "r_squared": float(r_sq),
                "n_countries": n_obs,
                "luxury_good": bool(beta[1] > 1.0),
            }

        # --- Wagner's law for health ---
        # Within-country: does public share of health spending rise with income?
        wagner = None
        pub_share_list, gdppc_list_w = [], []
        for iso in set(pub_data.keys()) & set(gdppc_data.keys()):
            years = sorted(set(pub_data[iso].keys()) & set(gdppc_data[iso].keys()))
            for yr in years:
                p_val = pub_data[iso][yr]
                g_val = gdppc_data[iso][yr]
                if p_val is not None and g_val and g_val > 0:
                    pub_share_list.append(p_val)
                    gdppc_list_w.append(np.log(g_val))

        if len(pub_share_list) >= 30:
            pub_arr = np.array(pub_share_list)
            gdp_arr = np.array(gdppc_list_w)
            X_w = np.column_stack([np.ones(len(gdp_arr)), gdp_arr])
            beta_w, _, _, _ = np.linalg.lstsq(X_w, pub_arr, rcond=None)
            y_hat_w = X_w @ beta_w
            ss_res_w = np.sum((pub_arr - y_hat_w) ** 2)
            ss_tot_w = np.sum((pub_arr - np.mean(pub_arr)) ** 2)
            r_sq_w = 1 - ss_res_w / ss_tot_w if ss_tot_w > 0 else 0

            wagner = {
                "coef_log_gdppc": float(beta_w[1]),
                "r_squared": float(r_sq_w),
                "n_obs": len(pub_arr),
                "wagner_holds": bool(beta_w[1] > 0),
            }

        # --- DEA efficiency frontier ---
        # Input: health expenditure per capita (THE% * GDPpc / 100)
        # Output: life expectancy
        dea_results = None
        dea_countries = []
        dea_inputs = []
        dea_outputs = []

        for iso in set(the_data.keys()) & set(gdppc_data.keys()) & set(le_data.keys()):
            years = sorted(
                set(the_data[iso].keys())
                & set(gdppc_data[iso].keys())
                & set(le_data[iso].keys())
            )
            if years:
                yr = years[-1]
                t_val = the_data[iso][yr]
                g_val = gdppc_data[iso][yr]
                l_val = le_data[iso][yr]
                if t_val and g_val and l_val and g_val > 0:
                    he_pc = t_val * g_val / 100.0
                    dea_countries.append(iso)
                    dea_inputs.append(he_pc)
                    dea_outputs.append(l_val)

        if len(dea_countries) >= 10:
            inp_arr = np.array(dea_inputs).reshape(-1, 1)
            out_arr = np.array(dea_outputs).reshape(-1, 1)
            eff_scores = _dea_output_oriented(inp_arr, out_arr)

            # Rank by efficiency
            ranked = sorted(zip(dea_countries, eff_scores.tolist(), dea_inputs, dea_outputs),
                            key=lambda x: -x[1])

            dea_results = {
                "n_countries": len(dea_countries),
                "frontier_countries": [c for c, s, _, _ in ranked if s >= 0.99],
                "mean_efficiency": float(np.mean(eff_scores)),
                "top_10": [
                    {"iso3": c, "efficiency": round(s, 4),
                     "he_per_capita": round(inp, 1), "life_expectancy": round(out, 1)}
                    for c, s, inp, out in ranked[:10]
                ],
            }

            if country_iso3 and country_iso3 in dea_countries:
                idx = dea_countries.index(country_iso3)
                dea_results["target"] = {
                    "efficiency": float(eff_scores[idx]),
                    "rank": int(np.sum(eff_scores >= eff_scores[idx])),
                    "he_per_capita": float(dea_inputs[idx]),
                    "life_expectancy": float(dea_outputs[idx]),
                }

        # --- Expenditure decomposition for target country ---
        decomposition = None
        if country_iso3:
            the_years = the_data.get(country_iso3, {})
            oop_years = oop_data.get(country_iso3, {})
            pub_years = pub_data.get(country_iso3, {})
            if the_years:
                latest = sorted(the_years.keys())[-1]
                total = the_years.get(latest, 0) or 0
                oop_share = oop_years.get(latest, 0) or 0
                pub_share = pub_years.get(latest, 0) or 0
                private_share = max(0, 100 - pub_share)
                decomposition = {
                    "year": latest,
                    "total_pct_gdp": float(total),
                    "public_share_pct": float(pub_share),
                    "private_share_pct": float(private_share),
                    "oop_share_pct": float(oop_share),
                    "oop_catastrophic_risk": oop_share > 40,
                }

        # --- Score ---
        # High OOP = stress, low efficiency = stress, low spending = watch
        score = 40  # baseline moderate
        if decomposition:
            if decomposition["oop_catastrophic_risk"]:
                score += 25
            elif decomposition["oop_share_pct"] > 30:
                score += 15
            if decomposition["total_pct_gdp"] < 3:
                score += 10
        if dea_results and dea_results.get("target"):
            eff = dea_results["target"]["efficiency"]
            if eff < 0.5:
                score += 15
            elif eff < 0.7:
                score += 8

        score = float(np.clip(score, 0, 100))

        results = {
            "newhouse_elasticity": newhouse,
            "wagner_health": wagner,
            "dea_efficiency": dea_results,
            "decomposition": decomposition,
            "country_iso3": country_iso3,
        }

        return {"score": score, "results": results}
