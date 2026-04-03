"""Exchange Rate Models module.

Methodology
-----------
1. **Mundell-Fleming IS-LM-BP**:
   - Small open economy with capital mobility
   - Under flexible rates: monetary policy effective, fiscal policy ineffective
     (due to exchange rate offset)
   - Under fixed rates: fiscal policy effective, monetary policy ineffective
     (trinity impossibility)
   - Estimate BP curve slope from capital flow / interest rate relationship

2. **Dornbusch Overshooting (1976)**:
   - Sticky prices + flexible exchange rates = overshooting
   - On monetary expansion, exchange rate depreciates beyond long-run level
   - Speed of adjustment depends on output sensitivity to real exchange rate
   - e(t) = e_bar + (e_0 - e_bar) * exp(-theta * t)
   - theta = rate of convergence to PPP

3. **Meese-Rogoff (1983) Challenge**:
   - Random walk beats structural models at short horizons
   - Compare RMSE of: random walk, PPP, monetary model, Dornbusch
   - Theil U-statistic: model_RMSE / random_walk_RMSE (< 1 = model wins)

4. **Carry Trade Returns**:
   - Borrow low-yield currency, invest in high-yield
   - Excess return = interest differential - exchange rate change
   - UIP violation: high-yield currencies tend NOT to depreciate enough
   - Sharpe ratio of carry trade strategy

Score reflects exchange rate misalignment and model fit.

Sources: FRED (DEXUSEU, DEXJPUS, DTWEXBGS, interest rate differentials)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ExchangeRateModels(LayerBase):
    layer_id = "l15"
    name = "Exchange Rate Models"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        partner = kwargs.get("partner", "EUR")
        lookback = kwargs.get("lookback_years", 20)

        series_codes = {
            "exchange_rate": f"EXRATE_{country}_{partner}",
            "reer": f"REER_{country}",
            "domestic_rate": f"POLICY_RATE_{country}",
            "foreign_rate": f"POLICY_RATE_{partner}",
            "inflation_dom": f"INFLATION_{country}",
            "inflation_for": f"INFLATION_{partner}",
            "gdp_dom": f"RGDP_{country}",
            "money_dom": f"M2_{country}",
            "money_for": f"M2_{partner}",
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

        if not data.get("exchange_rate"):
            return {"score": 50.0, "results": {"error": "insufficient exchange rate data"}}

        ex_dates = sorted(data["exchange_rate"])
        ex = np.array([data["exchange_rate"][d] for d in ex_dates])

        if len(ex) < 12:
            return {"score": 50.0, "results": {"error": "too few observations"}}

        results: dict = {
            "country": country,
            "partner": partner,
            "n_obs": len(ex),
            "period": f"{ex_dates[0]} to {ex_dates[-1]}",
        }

        ln_ex = np.log(np.maximum(ex, 1e-10))
        t = np.arange(len(ex), dtype=float)

        # --- 1. Mundell-Fleming IS-LM-BP ---
        if data.get("domestic_rate") and data.get("foreign_rate"):
            mf_dates = sorted(
                set(ex_dates) & set(data["domestic_rate"]) & set(data["foreign_rate"])
            )
            if len(mf_dates) >= 10:
                i_dom = np.array([data["domestic_rate"][d] for d in mf_dates])
                i_for = np.array([data["foreign_rate"][d] for d in mf_dates])
                ex_mf = np.array([data["exchange_rate"][d] for d in mf_dates])
                i_diff = i_dom - i_for

                # BP curve: capital flows respond to interest differential
                # Proxy: exchange rate changes vs interest differential
                d_ex = np.diff(np.log(np.maximum(ex_mf, 1e-10)))
                i_diff_lag = i_diff[:-1]

                if np.std(i_diff_lag) > 1e-10:
                    bp_slope = float(np.polyfit(i_diff_lag, d_ex, 1)[0])
                    bp_corr = float(np.corrcoef(i_diff_lag, d_ex)[0, 1])
                else:
                    bp_slope = 0.0
                    bp_corr = 0.0

                # Capital mobility classification
                if abs(bp_corr) > 0.5:
                    mobility = "high"
                elif abs(bp_corr) > 0.2:
                    mobility = "moderate"
                else:
                    mobility = "low"

                results["mundell_fleming"] = {
                    "interest_differential_latest": round(float(i_diff[-1]), 3),
                    "bp_slope": round(bp_slope, 4),
                    "bp_correlation": round(bp_corr, 3),
                    "capital_mobility": mobility,
                    "n_obs": len(mf_dates),
                }
            else:
                results["mundell_fleming"] = {"note": "insufficient rate data"}
        else:
            results["mundell_fleming"] = {"note": "interest rate data unavailable"}

        # --- 2. Dornbusch Overshooting ---
        # Test: after large exchange rate moves, does rate revert?
        # Estimate mean reversion speed: d(ln_e) = -theta * (ln_e - ln_e_bar) + eps
        ln_e_bar = float(np.mean(ln_ex))
        deviation = ln_ex - ln_e_bar

        if len(deviation) > 5:
            d_ln_ex = np.diff(ln_ex)
            dev_lag = deviation[:-1]

            if np.std(dev_lag) > 1e-10:
                # OLS: d_ln_ex = alpha + theta * dev_lag + e
                X_dorn = np.column_stack([np.ones(len(dev_lag)), dev_lag])
                beta_dorn = np.linalg.lstsq(X_dorn, d_ln_ex, rcond=None)[0]
                theta = -float(beta_dorn[1])  # negative sign: mean reversion
                resid_dorn = d_ln_ex - X_dorn @ beta_dorn
                se_dorn = np.sqrt(np.diag(
                    float(np.sum(resid_dorn ** 2)) / max(len(d_ln_ex) - 2, 1)
                    * np.linalg.inv(X_dorn.T @ X_dorn)
                ))

                half_life = float(np.log(2) / theta) if theta > 0.01 else None

                # Overshooting test: large moves followed by partial reversion
                large_moves = np.abs(d_ln_ex) > 2 * np.std(d_ln_ex)
                if np.sum(large_moves) > 2:
                    next_moves = np.zeros(np.sum(large_moves))
                    idx = 0
                    for j in range(len(d_ln_ex)):
                        if large_moves[j] and j + 1 < len(d_ln_ex):
                            next_moves[idx] = d_ln_ex[j + 1]
                            idx += 1
                    reversion_frac = float(
                        np.mean(np.sign(d_ln_ex[large_moves]) != np.sign(next_moves[:idx]))
                    ) if idx > 0 else 0.0
                else:
                    reversion_frac = None

                results["dornbusch"] = {
                    "theta": round(theta, 4),
                    "theta_se": round(float(se_dorn[1]), 4),
                    "half_life_periods": round(half_life, 1) if half_life is not None else None,
                    "mean_reverting": theta > 0 and float(se_dorn[1]) > 0 and theta / float(se_dorn[1]) > 1.65,
                    "current_deviation_pct": round(float(deviation[-1]) * 100, 2),
                    "reversion_after_large_moves": (
                        round(reversion_frac, 3) if reversion_frac is not None else None
                    ),
                    "overshooting_evidence": (
                        reversion_frac > 0.5 if reversion_frac is not None else None
                    ),
                }
            else:
                results["dornbusch"] = {"note": "insufficient variation"}
        else:
            results["dornbusch"] = {"note": "insufficient data"}

        # --- 3. Meese-Rogoff Forecasting Challenge ---
        # Compare random walk vs structural model at various horizons
        if len(ln_ex) >= 20:
            horizons = [1, 4, 8]
            forecast_results = {}

            for h in horizons:
                if len(ln_ex) < h + 10:
                    continue
                # Random walk forecast: e_{t+h} = e_t
                rw_forecast = ln_ex[:-h]
                actual = ln_ex[h:]
                n_fc = min(len(rw_forecast), len(actual))
                rw_forecast = rw_forecast[:n_fc]
                actual = actual[:n_fc]

                rw_rmse = float(np.sqrt(np.mean((actual - rw_forecast) ** 2)))

                # PPP model: ln(e_{t+h}) = ln(e_t) + (pi_dom - pi_for) * h
                if data.get("inflation_dom") and data.get("inflation_for"):
                    inf_dom_arr = []
                    inf_for_arr = []
                    for i_fc in range(n_fc):
                        d = ex_dates[i_fc]
                        inf_dom_arr.append(data["inflation_dom"].get(d, 0.0))
                        inf_for_arr.append(data["inflation_for"].get(d, 0.0))
                    inf_dom_arr = np.array(inf_dom_arr)
                    inf_for_arr = np.array(inf_for_arr)
                    ppp_forecast = ln_ex[:n_fc] + (inf_dom_arr - inf_for_arr) * h / 400
                    ppp_rmse = float(np.sqrt(np.mean((actual - ppp_forecast) ** 2)))
                    theil_u_ppp = ppp_rmse / rw_rmse if rw_rmse > 1e-10 else None
                else:
                    ppp_rmse = None
                    theil_u_ppp = None

                # Monetary model: ln(e) = (m - m*) - phi*(y - y*)
                if data.get("money_dom") and data.get("money_for"):
                    m_dom_arr = []
                    m_for_arr = []
                    for i_fc in range(n_fc):
                        d = ex_dates[i_fc]
                        m_dom_arr.append(np.log(max(data["money_dom"].get(d, 1.0), 1e-10)))
                        m_for_arr.append(np.log(max(data["money_for"].get(d, 1.0), 1e-10)))
                    m_dom_arr = np.array(m_dom_arr)
                    m_for_arr = np.array(m_for_arr)
                    # Simple monetary: just money differential
                    mon_forecast = m_dom_arr - m_for_arr
                    # Scale to match mean
                    mon_forecast = mon_forecast - np.mean(mon_forecast) + np.mean(actual)
                    mon_rmse = float(np.sqrt(np.mean((actual - mon_forecast) ** 2)))
                    theil_u_mon = mon_rmse / rw_rmse if rw_rmse > 1e-10 else None
                else:
                    mon_rmse = None
                    theil_u_mon = None

                forecast_results[f"h{h}"] = {
                    "random_walk_rmse": round(rw_rmse, 6),
                    "ppp_rmse": round(ppp_rmse, 6) if ppp_rmse is not None else None,
                    "ppp_theil_u": round(theil_u_ppp, 3) if theil_u_ppp is not None else None,
                    "monetary_rmse": round(mon_rmse, 6) if mon_rmse is not None else None,
                    "monetary_theil_u": round(theil_u_mon, 3) if theil_u_mon is not None else None,
                    "random_walk_wins": all(
                        u is None or u >= 1.0 for u in [theil_u_ppp, theil_u_mon]
                    ),
                }

            results["meese_rogoff"] = forecast_results if forecast_results else {"note": "insufficient data"}
        else:
            results["meese_rogoff"] = {"note": "insufficient data"}

        # --- 4. Carry Trade Returns ---
        if data.get("domestic_rate") and data.get("foreign_rate"):
            ct_dates = sorted(
                set(ex_dates) & set(data["domestic_rate"]) & set(data["foreign_rate"])
            )
            if len(ct_dates) >= 10:
                i_dom_ct = np.array([data["domestic_rate"][d] for d in ct_dates])
                i_for_ct = np.array([data["foreign_rate"][d] for d in ct_dates])
                ex_ct = np.array([data["exchange_rate"][d] for d in ct_dates])

                # Carry = interest differential
                carry = (i_dom_ct - i_for_ct) / 400  # quarterly rate

                # FX return (log)
                fx_return = np.diff(np.log(np.maximum(ex_ct, 1e-10)))

                # Carry trade excess return = carry - fx depreciation
                n_ct = min(len(carry) - 1, len(fx_return))
                excess_return = carry[:n_ct] - fx_return[:n_ct]

                mean_excess = float(np.mean(excess_return))
                std_excess = float(np.std(excess_return, ddof=1))
                sharpe = mean_excess / std_excess if std_excess > 1e-10 else 0.0

                # UIP test: regress fx_return on interest differential
                # Under UIP: beta = 1. Typical finding: beta < 1 (forward premium puzzle)
                if np.std(carry[:n_ct]) > 1e-10:
                    uip_beta = float(np.polyfit(carry[:n_ct], fx_return[:n_ct], 1)[0])
                else:
                    uip_beta = 0.0

                # Crash risk: skewness of excess returns
                skew = float(
                    np.mean(((excess_return - mean_excess) / max(std_excess, 1e-10)) ** 3)
                )

                results["carry_trade"] = {
                    "mean_excess_return": round(mean_excess * 400, 3),  # annualized bp
                    "volatility": round(std_excess * 400, 3),
                    "sharpe_ratio": round(sharpe * np.sqrt(4), 3),  # annualized
                    "uip_beta": round(uip_beta, 3),
                    "uip_holds": 0.5 < uip_beta < 1.5,
                    "forward_premium_puzzle": uip_beta < 0.5,
                    "skewness": round(skew, 3),
                    "crash_risk": skew < -0.5,
                }
            else:
                results["carry_trade"] = {"note": "insufficient rate data"}
        else:
            results["carry_trade"] = {"note": "rate data unavailable"}

        # --- Score ---
        # Misalignment + random walk dominance + UIP failure -> stress
        misalignment_penalty = 0.0
        if "dornbusch" in results and isinstance(results["dornbusch"].get("current_deviation_pct"), (int, float)):
            misalignment_penalty = min(abs(results["dornbusch"]["current_deviation_pct"]) * 1.5, 25)

        forecast_penalty = 0.0
        if isinstance(results.get("meese_rogoff"), dict):
            rw_wins = sum(
                1 for v in results["meese_rogoff"].values()
                if isinstance(v, dict) and v.get("random_walk_wins")
            )
            forecast_penalty = rw_wins * 8

        carry_penalty = 0.0
        if isinstance(results.get("carry_trade"), dict) and results["carry_trade"].get("crash_risk"):
            carry_penalty = 15.0

        uip_penalty = 0.0
        if isinstance(results.get("carry_trade"), dict) and results["carry_trade"].get("forward_premium_puzzle"):
            uip_penalty = 10.0

        mean_revert_bonus = 0.0
        if isinstance(results.get("dornbusch"), dict) and results["dornbusch"].get("mean_reverting"):
            mean_revert_bonus = -10.0

        score = np.clip(
            misalignment_penalty + forecast_penalty + carry_penalty + uip_penalty + mean_revert_bonus + 20,
            0, 100,
        )

        return {"score": round(float(score), 1), "results": results}
