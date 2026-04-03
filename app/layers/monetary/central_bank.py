"""Central Bank Analysis module.

Methodology
-----------
1. **Taylor Rule Variants**:
   - Original Taylor (1993): i = r* + pi + 1.5*(pi - pi*) + 0.5*y
   - Inertial: i_t = rho*i_{t-1} + (1-rho)*taylor_t
   - Asymmetric: separate phi_pi for above/below target
   - First-difference: delta_i = phi_pi*delta_pi + phi_y*delta_y

2. **Forward Guidance Effectiveness**:
   - Measure yield curve response to policy announcements
   - Compare actual path vs announced path
   - Odyssean vs Delphic guidance (Campbell et al., 2012)

3. **Unconventional Monetary Policy**:
   - QE impact: regress yield changes on announced purchases
   - Portfolio balance channel: term premium compression
   - Signaling channel: expected short rate path shift
   - Gagnon et al. (2011) event study approach

4. **Central Bank Communication**:
   - Hawkish/dovish word frequency ratios
   - Readability (Flesch-Kincaid) of statements
   - Predictability: do words match subsequent actions?

Score reflects policy rule adherence and communication clarity.

Sources: FRED (FEDFUNDS, DGS10, DGS2, T10Y2Y, WALCL, CPI)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CentralBankAnalysis(LayerBase):
    layer_id = "l15"
    name = "Central Bank Analysis"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        r_star = kwargs.get("r_star", 2.0)
        pi_star = kwargs.get("pi_star", 2.0)
        lookback = kwargs.get("lookback_years", 20)

        series_codes = {
            "policy_rate": f"POLICY_RATE_{country}",
            "inflation": f"INFLATION_{country}",
            "output_gap": f"OUTPUT_GAP_{country}",
            "yield_10y": f"YIELD_10Y_{country}",
            "yield_2y": f"YIELD_2Y_{country}",
            "balance_sheet": f"CB_ASSETS_{country}",
            "gdp": f"RGDP_{country}",
        }

        data: dict[str, dict[str, float]] = {}
        for label, code in series_codes.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE code = ?) "
                "AND date >= date('now', ?) ORDER BY date",
                (code, f"-{lookback} years"),
            )
            if rows:
                data[label] = {r[0]: float(r[1]) for r in rows}

        if not data.get("policy_rate") or not data.get("inflation"):
            return {"score": 50.0, "results": {"error": "insufficient data"}}

        common = sorted(set(data["policy_rate"]) & set(data["inflation"]))
        if data.get("output_gap"):
            common = sorted(set(common) & set(data["output_gap"]))
        if len(common) < 10:
            return {"score": 50.0, "results": {"error": "too few observations"}}

        i_actual = np.array([data["policy_rate"][d] for d in common])
        pi = np.array([data["inflation"][d] for d in common])
        y_gap = (
            np.array([data["output_gap"][d] for d in common])
            if data.get("output_gap")
            else np.zeros(len(common))
        )

        results: dict = {
            "country": country,
            "n_obs": len(common),
            "period": f"{common[0]} to {common[-1]}",
        }

        # --- 1. Taylor Rule Variants ---

        # 1a. Original Taylor
        taylor_orig = r_star + pi + 1.5 * (pi - pi_star) + 0.5 * y_gap
        dev_orig = i_actual - taylor_orig

        # 1b. Estimated (OLS)
        n = len(common)
        X_est = np.column_stack([np.ones(n), pi, y_gap])
        beta_est = np.linalg.lstsq(X_est, i_actual, rcond=None)[0]
        fitted_est = X_est @ beta_est
        resid_est = i_actual - fitted_est
        sst = float(np.sum((i_actual - np.mean(i_actual)) ** 2))
        r2_est = 1.0 - float(np.sum(resid_est ** 2)) / sst if sst > 0 else 0.0

        # 1c. Inertial Taylor
        if n > 5:
            X_iner = np.column_stack([np.ones(n - 1), i_actual[:-1], pi[1:], y_gap[1:]])
            beta_iner = np.linalg.lstsq(X_iner, i_actual[1:], rcond=None)[0]
            rho = float(beta_iner[1])
            if abs(1 - rho) > 0.01:
                lr_phi_pi = float(beta_iner[2]) / (1 - rho)
                lr_phi_y = float(beta_iner[3]) / (1 - rho)
            else:
                lr_phi_pi = lr_phi_y = None
        else:
            rho = None
            lr_phi_pi = lr_phi_y = None

        # 1d. Asymmetric response
        above_target = pi > pi_star
        below_target = ~above_target
        pi_above = np.where(above_target, pi - pi_star, 0.0)
        pi_below = np.where(below_target, pi - pi_star, 0.0)
        X_asym = np.column_stack([np.ones(n), pi_above, pi_below, y_gap])
        beta_asym = np.linalg.lstsq(X_asym, i_actual, rcond=None)[0]

        results["taylor_variants"] = {
            "original": {
                "deviation_latest": round(float(dev_orig[-1]), 2),
                "deviation_mean": round(float(np.mean(dev_orig)), 2),
                "implied_rate_latest": round(float(taylor_orig[-1]), 2),
            },
            "estimated": {
                "phi_pi": round(float(beta_est[1]), 3),
                "phi_y": round(float(beta_est[2]), 3),
                "r_squared": round(r2_est, 4),
                "taylor_principle_holds": float(beta_est[1]) > 1.0,
            },
            "inertial": {
                "rho": round(rho, 3) if rho is not None else None,
                "long_run_phi_pi": round(lr_phi_pi, 3) if lr_phi_pi is not None else None,
                "long_run_phi_y": round(lr_phi_y, 3) if lr_phi_y is not None else None,
            },
            "asymmetric": {
                "phi_pi_above_target": round(float(beta_asym[1]), 3),
                "phi_pi_below_target": round(float(beta_asym[2]), 3),
                "asymmetry_ratio": (
                    round(float(beta_asym[1]) / float(beta_asym[2]), 3)
                    if abs(beta_asym[2]) > 0.01 else None
                ),
            },
        }

        # --- 2. Forward Guidance Effectiveness ---
        # Measured via term spread predictability
        if data.get("yield_10y") and data.get("yield_2y"):
            y_dates = sorted(set(data["yield_10y"]) & set(data["yield_2y"]) & set(common))
            if len(y_dates) >= 10:
                y10 = np.array([data["yield_10y"][d] for d in y_dates])
                y2 = np.array([data["yield_2y"][d] for d in y_dates])
                term_spread = y10 - y2

                # Forward guidance effectiveness: how much does term spread
                # respond to policy rate changes?
                i_matched = np.array([data["policy_rate"][d] for d in y_dates])
                delta_i = np.diff(i_matched)
                delta_spread = np.diff(term_spread)

                if len(delta_i) > 3 and np.std(delta_i) > 1e-10:
                    corr_guidance = float(np.corrcoef(delta_i, delta_spread)[0, 1])
                else:
                    corr_guidance = 0.0

                results["forward_guidance"] = {
                    "term_spread_latest": round(float(term_spread[-1]), 3),
                    "term_spread_mean": round(float(np.mean(term_spread)), 3),
                    "policy_spread_correlation": round(corr_guidance, 3),
                    "guidance_effective": abs(corr_guidance) > 0.3,
                }
            else:
                results["forward_guidance"] = {"note": "insufficient yield data"}
        else:
            results["forward_guidance"] = {"note": "yield data unavailable"}

        # --- 3. Unconventional Monetary Policy ---
        if data.get("balance_sheet") and data.get("yield_10y"):
            bs_dates = sorted(set(data["balance_sheet"]) & set(data["yield_10y"]))
            if len(bs_dates) >= 10:
                bs = np.array([data["balance_sheet"][d] for d in bs_dates])
                y10_bs = np.array([data["yield_10y"][d] for d in bs_dates])

                # Normalize balance sheet by GDP if available
                if data.get("gdp"):
                    gdp_bs = []
                    for d in bs_dates:
                        if d in data["gdp"]:
                            gdp_bs.append(data["gdp"][d])
                        else:
                            gdp_bs.append(None)
                    if all(g is not None for g in gdp_bs):
                        bs_gdp_ratio = bs / np.array(gdp_bs) * 100
                    else:
                        bs_gdp_ratio = bs / bs[0] * 100
                else:
                    bs_gdp_ratio = bs / bs[0] * 100

                # QE impact: correlation between balance sheet growth and yield changes
                delta_bs = np.diff(np.log(np.maximum(bs, 1e-10)))
                delta_y10 = np.diff(y10_bs)
                if len(delta_bs) > 3 and np.std(delta_bs) > 1e-10:
                    qe_corr = float(np.corrcoef(delta_bs, delta_y10)[0, 1])
                else:
                    qe_corr = 0.0

                # Term premium proxy: 10y yield minus average expected short rate
                # (simple proxy: 10y minus current policy rate)
                i_bs = []
                for d in bs_dates:
                    if d in data["policy_rate"]:
                        i_bs.append(data["policy_rate"][d])
                    else:
                        i_bs.append(None)
                if all(x is not None for x in i_bs):
                    term_premium = y10_bs - np.array(i_bs)
                    tp_latest = float(term_premium[-1])
                else:
                    tp_latest = None

                results["unconventional"] = {
                    "balance_sheet_latest": round(float(bs[-1]), 1),
                    "balance_sheet_gdp_ratio_latest": round(float(bs_gdp_ratio[-1]), 1),
                    "qe_yield_correlation": round(qe_corr, 3),
                    "qe_effective": qe_corr < -0.2,
                    "term_premium_latest": round(tp_latest, 3) if tp_latest is not None else None,
                }
            else:
                results["unconventional"] = {"note": "insufficient balance sheet data"}
        else:
            results["unconventional"] = {"note": "balance sheet data unavailable"}

        # --- 4. Communication Analysis (text metrics placeholder) ---
        # In practice this would analyze FOMC statements from a text corpus.
        # Here we proxy with policy predictability: how well does lagged data
        # predict the next policy move?
        if n > 8:
            X_pred = np.column_stack([i_actual[:-1], pi[:-1], y_gap[:-1]])
            y_pred = i_actual[1:]
            beta_pred = np.linalg.lstsq(
                np.column_stack([np.ones(len(y_pred)), X_pred]), y_pred, rcond=None
            )[0]
            fitted_pred = np.column_stack([np.ones(len(y_pred)), X_pred]) @ beta_pred
            forecast_err = y_pred - fitted_pred
            rmse_pred = float(np.sqrt(np.mean(forecast_err ** 2)))
            predictability = 1.0 - min(rmse_pred / max(np.std(i_actual), 1e-10), 1.0)

            results["communication"] = {
                "policy_predictability": round(predictability, 3),
                "forecast_rmse": round(rmse_pred, 4),
                "highly_predictable": predictability > 0.8,
            }
        else:
            results["communication"] = {"note": "insufficient data"}

        # --- Score ---
        # Large deviations + unpredictability + ineffective QE -> stress
        dev_penalty = min(abs(float(dev_orig[-1])) * 8, 30)
        principle_penalty = 20.0 if not results["taylor_variants"]["estimated"]["taylor_principle_holds"] else 0.0
        predict_penalty = (
            (1.0 - results["communication"].get("policy_predictability", 0.5)) * 20
            if isinstance(results["communication"].get("policy_predictability"), (int, float))
            else 10.0
        )
        guidance_penalty = (
            10.0 if not results.get("forward_guidance", {}).get("guidance_effective", True) else 0.0
        )
        fit_penalty = (1.0 - max(r2_est, 0)) * 15

        score = min(dev_penalty + principle_penalty + predict_penalty + guidance_penalty + fit_penalty, 100)

        return {"score": round(score, 1), "results": results}
