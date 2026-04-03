"""Exchange Rate Pass-Through (ERPT) estimation module.

Methodology
-----------
ERPT measures how exchange rate changes transmit to domestic prices.

**Single-equation approach** (Campa & Goldberg 2005):
    delta_p_t = alpha + beta_0 * delta_e_t + beta_1 * delta_e_{t-1}
                + ... + beta_k * delta_e_{t-k}
                + gamma * delta_y_t + delta * delta_p*_t + eps_t

where:
    p_t  = log domestic price index (import prices or CPI)
    e_t  = log nominal exchange rate (domestic currency per USD)
    y_t  = log output gap or industrial production (demand control)
    p*_t = log foreign price level (cost control)

Short-run ERPT = beta_0
Long-run ERPT = sum(beta_j) for j=0..k

ERPT to import prices is typically higher (0.5-0.8) than to consumer prices
(0.1-0.3), reflecting distribution margins, local costs, and pricing-to-market.

**VAR approach** (McCarthy 2007):
    5-variable recursive VAR: [oil, output_gap, exchange_rate, import_prices, cpi]
    Cholesky identification with supply -> demand -> exchange rate ordering.
    Cumulative IRFs give dynamic pass-through coefficients.

**Key findings in literature**:
- ERPT has declined in many countries since 1990s (Taylor 2000 hypothesis:
  low-inflation environment reduces ERPT)
- ERPT is asymmetric: depreciations pass through more than appreciations
- ERPT varies by product category and degree of import competition

Score reflects ERPT magnitude and stability. High/unstable ERPT increases
inflation vulnerability to exchange rate shocks.

Sources: FRED, IMF IFS, BLS import price indices
"""

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class ExchangeRatePassThrough(LayerBase):
    layer_id = "l2"
    name = "Exchange Rate Pass-Through"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        n_lags = kwargs.get("n_lags", 4)
        var_lags = kwargs.get("var_lags", 4)

        # Fetch data
        series_map = {
            "exchange_rate": f"NEER_{country}",
            "import_prices": f"IMPORT_PRICES_{country}",
            "cpi": f"CPI_{country}",
            "output_gap": f"OUTPUT_GAP_{country}",
            "foreign_prices": f"FOREIGN_CPI_{country}",
            "oil_price": "OIL_PRICE_WTI",
        }
        data = {}
        for label, code in series_map.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
                (code,),
            )
            if rows:
                data[label] = {r[0]: float(r[1]) for r in rows}

        if not data.get("exchange_rate"):
            return {"score": 50, "results": {"error": "no exchange rate data"}}

        results = {"country": country}

        # --- Single-equation ERPT to import prices ---
        for price_label, price_name in [("import_prices", "Import Prices"), ("cpi", "CPI")]:
            if price_label not in data:
                continue

            common = sorted(set(data["exchange_rate"]) & set(data[price_label]))
            controls = {}
            if "output_gap" in data:
                common = sorted(set(common) & set(data["output_gap"]))
                controls["output_gap"] = data["output_gap"]
            if "foreign_prices" in data:
                common = sorted(set(common) & set(data["foreign_prices"]))
                controls["foreign_prices"] = data["foreign_prices"]

            if len(common) < n_lags + 15:
                results[f"erpt_{price_label}"] = {"error": "insufficient data"}
                continue

            e = np.array([np.log(data["exchange_rate"][d]) for d in common])
            p = np.array([np.log(data[price_label][d]) for d in common])

            # First differences
            de = np.diff(e)
            dp = np.diff(p)
            dates_diff = common[1:]

            # Build regression matrix
            # Need n_lags of lagged exchange rate changes
            effective_n = len(de) - n_lags
            if effective_n < 10:
                results[f"erpt_{price_label}"] = {"error": "insufficient data after lagging"}
                continue

            Y = dp[n_lags:]
            X_parts = [np.ones((effective_n, 1))]

            # Current and lagged exchange rate changes
            for lag in range(n_lags + 1):
                X_parts.append(de[n_lags - lag : len(de) - lag].reshape(-1, 1))

            # Control variables (in changes)
            for ctrl_name, ctrl_data in controls.items():
                ctrl_vals = np.array([np.log(ctrl_data[d]) for d in common])
                d_ctrl = np.diff(ctrl_vals)
                X_parts.append(d_ctrl[n_lags:].reshape(-1, 1))

            X = np.hstack(X_parts)
            n_obs = len(Y)

            beta = np.linalg.lstsq(X, Y, rcond=None)[0]
            resid = Y - X @ beta
            sse = float(np.sum(resid ** 2))
            sst = float(np.sum((Y - np.mean(Y)) ** 2))
            r_squared = 1 - sse / sst if sst > 0 else 0.0

            # HC1 SE
            k_params = X.shape[1]
            bread = np.linalg.inv(X.T @ X)
            meat = X.T @ np.diag(resid ** 2) @ X
            vcov = (n_obs / (n_obs - k_params)) * bread @ meat @ bread
            se = np.sqrt(np.diag(vcov))

            # ERPT coefficients (skip intercept, start at index 1)
            erpt_coeffs = beta[1 : n_lags + 2]
            erpt_se = se[1 : n_lags + 2]

            short_run_erpt = float(erpt_coeffs[0])
            long_run_erpt = float(np.sum(erpt_coeffs))

            # Wald test for long-run ERPT = 1 (complete pass-through)
            R = np.zeros((1, k_params))
            R[0, 1 : n_lags + 2] = 1
            wald_num = (R @ beta - 1) ** 2
            wald_den = R @ vcov @ R.T
            wald_stat = float(wald_num / wald_den) if wald_den > 0 else 0
            wald_p = 1 - stats.chi2.cdf(wald_stat, 1)

            results[f"erpt_{price_label}"] = {
                "price_index": price_name,
                "n_obs": n_obs,
                "n_lags": n_lags,
                "short_run_erpt": round(short_run_erpt, 4),
                "short_run_se": round(float(erpt_se[0]), 4),
                "long_run_erpt": round(long_run_erpt, 4),
                "lag_coefficients": [round(float(c), 4) for c in erpt_coeffs],
                "lag_se": [round(float(s), 4) for s in erpt_se],
                "r_squared": round(r_squared, 4),
                "complete_pt_test": {
                    "wald_stat": round(wald_stat, 3),
                    "p_value": round(float(wald_p), 4),
                    "rejects_complete_pt": float(wald_p) < 0.05,
                },
            }

        # --- Asymmetry test ---
        # Split exchange rate changes into appreciation and depreciation
        if "exchange_rate" in data and ("import_prices" in data or "cpi" in data):
            price_key = "import_prices" if "import_prices" in data else "cpi"
            common_asym = sorted(set(data["exchange_rate"]) & set(data[price_key]))

            if len(common_asym) > 20:
                e_asym = np.array([np.log(data["exchange_rate"][d]) for d in common_asym])
                p_asym = np.array([np.log(data[price_key][d]) for d in common_asym])
                de_asym = np.diff(e_asym)
                dp_asym = np.diff(p_asym)

                # Split: depreciation (de > 0 for domestic/USD convention) vs appreciation
                dep_mask = de_asym > 0
                app_mask = de_asym <= 0

                de_dep = de_asym * dep_mask  # zero when appreciation
                de_app = de_asym * app_mask  # zero when depreciation

                n_a = len(dp_asym)
                X_asym = np.column_stack([np.ones(n_a), de_dep, de_app])
                beta_asym = np.linalg.lstsq(X_asym, dp_asym, rcond=None)[0]
                resid_asym = dp_asym - X_asym @ beta_asym

                # Test beta_dep = beta_app
                bread_a = np.linalg.inv(X_asym.T @ X_asym)
                meat_a = X_asym.T @ np.diag(resid_asym ** 2) @ X_asym
                vcov_a = (n_a / (n_a - 3)) * bread_a @ meat_a @ bread_a

                diff = float(beta_asym[1] - beta_asym[2])
                diff_var = float(vcov_a[1, 1] + vcov_a[2, 2] - 2 * vcov_a[1, 2])
                diff_se = float(np.sqrt(max(diff_var, 0)))
                t_asym = diff / diff_se if diff_se > 0 else 0

                results["asymmetry"] = {
                    "depreciation_erpt": round(float(beta_asym[1]), 4),
                    "appreciation_erpt": round(float(beta_asym[2]), 4),
                    "difference": round(diff, 4),
                    "t_stat": round(t_asym, 3),
                    "p_value": round(float(2 * (1 - stats.t.cdf(abs(t_asym), n_a - 3))), 4),
                    "asymmetric": abs(t_asym) > 1.96,
                }

        # --- VAR approach (if oil + output gap + exchange rate + prices available) ---
        var_vars = ["oil_price", "output_gap", "exchange_rate"]
        price_for_var = "import_prices" if "import_prices" in data else "cpi" if "cpi" in data else None

        if price_for_var and all(v in data for v in var_vars):
            var_vars_full = var_vars + [price_for_var]
            if "cpi" in data and price_for_var == "import_prices":
                var_vars_full.append("cpi")

            common_var = sorted(set.intersection(*[set(data[v]) for v in var_vars_full]))

            if len(common_var) > var_lags + 20:
                Z = np.column_stack([
                    np.diff(np.log(np.array([data[v][d] for d in common_var])))
                    for v in var_vars_full
                ])

                T_var, k_var = Z.shape
                n_var = T_var - var_lags
                Y_var = Z[var_lags:]
                X_parts = [np.ones((n_var, 1))]
                for lag in range(1, var_lags + 1):
                    X_parts.append(Z[var_lags - lag : T_var - lag])
                X_var = np.hstack(X_parts)

                B_var = np.linalg.lstsq(X_var, Y_var, rcond=None)[0]
                resid_var = Y_var - X_var @ B_var
                sigma_var = (resid_var.T @ resid_var) / n_var
                chol = np.linalg.cholesky(sigma_var)

                # Extract lag matrices
                A_list = []
                for lag in range(var_lags):
                    A_list.append(B_var[1 + lag * k_var : 1 + (lag + 1) * k_var, :].T)

                # Companion matrix
                companion = np.zeros((k_var * var_lags, k_var * var_lags))
                for lag in range(var_lags):
                    companion[:k_var, lag * k_var : (lag + 1) * k_var] = A_list[lag]
                if var_lags > 1:
                    companion[k_var:, : k_var * (var_lags - 1)] = np.eye(k_var * (var_lags - 1))

                # IRF: exchange rate shock (index 2) -> prices
                irf_horizon_var = 20
                irf = np.zeros((irf_horizon_var + 1, k_var))
                state = np.zeros(k_var * var_lags)
                state[:k_var] = chol[:, 2]  # exchange rate shock
                irf[0] = state[:k_var]
                for h in range(1, irf_horizon_var + 1):
                    state = companion @ state
                    irf[h] = state[:k_var]

                # Cumulative ERPT from VAR
                cum_irf = np.cumsum(irf, axis=0)
                cum_er = cum_irf[:, 2]  # cumulative exchange rate response

                var_erpt_results = {"horizon": irf_horizon_var}
                for idx, vname in enumerate(var_vars_full):
                    if vname in ("import_prices", "cpi"):
                        erpt_dynamic = []
                        for h in range(irf_horizon_var + 1):
                            if abs(cum_er[h]) > 1e-10:
                                erpt_dynamic.append(round(float(cum_irf[h, idx] / cum_er[h]), 4))
                            else:
                                erpt_dynamic.append(0.0)
                        var_erpt_results[f"erpt_to_{vname}"] = {
                            "dynamic_erpt": erpt_dynamic,
                            "impact": erpt_dynamic[0] if erpt_dynamic else None,
                            "long_run": erpt_dynamic[-1] if erpt_dynamic else None,
                        }

                results["var_erpt"] = var_erpt_results

        # --- Rolling ERPT (time-varying) ---
        if f"erpt_import_prices" in results or f"erpt_cpi" in results:
            price_key_roll = "import_prices" if "import_prices" in data else "cpi"
            common_roll = sorted(set(data["exchange_rate"]) & set(data[price_key_roll]))

            if len(common_roll) > 40:
                e_roll = np.array([np.log(data["exchange_rate"][d]) for d in common_roll])
                p_roll = np.array([np.log(data[price_key_roll][d]) for d in common_roll])
                de_roll = np.diff(e_roll)
                dp_roll = np.diff(p_roll)

                win = kwargs.get("rolling_window", 20)
                rolling_erpt = []
                rolling_dates_out = []
                for i in range(len(de_roll) - win + 1):
                    de_w = de_roll[i : i + win]
                    dp_w = dp_roll[i : i + win]
                    X_w = np.column_stack([np.ones(win), de_w])
                    b_w = np.linalg.lstsq(X_w, dp_w, rcond=None)[0]
                    rolling_erpt.append(round(float(b_w[1]), 4))
                    rolling_dates_out.append(common_roll[i + win])

                results["rolling_erpt"] = {
                    "window": win,
                    "price_index": price_key_roll,
                    "values": rolling_erpt,
                    "dates": rolling_dates_out,
                    "trend": "declining" if rolling_erpt[-1] < rolling_erpt[0] else "increasing",
                }

        # --- Score ---
        # High ERPT to CPI = inflation vulnerability
        cpi_erpt = 0.0
        if "erpt_cpi" in results and isinstance(results["erpt_cpi"], dict):
            cpi_erpt = abs(results["erpt_cpi"].get("long_run_erpt", 0))

        import_erpt = 0.0
        if "erpt_import_prices" in results and isinstance(results["erpt_import_prices"], dict):
            import_erpt = abs(results["erpt_import_prices"].get("long_run_erpt", 0))

        # High CPI pass-through -> inflation vulnerability
        cpi_penalty = min(cpi_erpt * 50, 35)
        # Very high import price pass-through
        import_penalty = min(max(import_erpt - 0.5, 0) * 30, 20)
        # Asymmetry adds risk
        asym_penalty = 10 if results.get("asymmetry", {}).get("asymmetric", False) else 0
        # Increasing ERPT trend
        trend_penalty = 10 if results.get("rolling_erpt", {}).get("trend") == "increasing" else 0

        score = min(cpi_penalty + import_penalty + asym_penalty + trend_penalty, 100)

        results["n_obs"] = len(common) if "common" in dir() else 0
        results["period"] = f"{common[0]} to {common[-1]}" if "common" in dir() and common else "N/A"

        return {"score": round(score, 1), "results": results}
