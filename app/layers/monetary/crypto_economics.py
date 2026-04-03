"""Crypto Economics module.

Methodology
-----------
1. **Bitcoin Price Dynamics (Stock-to-Flow)**:
   - PlanB (2019) model: ln(P) = a + b * ln(S2F)
   - S2F = stock / annual flow (current supply / annual issuance)
   - After halvings, S2F roughly doubles, predicting price increase
   - Cross-asset validation: gold S2F ~ 62, silver ~ 22
   - Model fit and residual analysis for bubble/crash detection

2. **DeFi Yield Analysis**:
   - Decompose yield into: base rate + liquidity premium + smart contract risk
   - Sustainable yield proxy: protocol revenue / TVL
   - Excess yield = offered APY - sustainable yield (Ponzi indicator)
   - Impermanent loss estimation for AMM LPs

3. **Stablecoin Depegging Risk**:
   - Collateralization ratio monitoring
   - Peg deviation: |price - 1.0| tracking
   - Redemption pressure: outflow velocity
   - Reserve composition quality score

4. **CBDC Impact on Banking Deposits**:
   - Substitution elasticity: CBDC adoption vs bank deposit growth
   - Disintermediation risk: deposit flight scenarios
   - Brunnermeier and Niepelt (2019) equivalence theorem conditions
   - Two-tier system design effectiveness

Score reflects crypto market stability and systemic risk.

Sources: Data series for crypto prices, DeFi TVL, stablecoin metrics
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class CryptoEconomics(LayerBase):
    layer_id = "l15"
    name = "Crypto Economics"

    async def compute(self, db, **kwargs) -> dict:
        lookback = kwargs.get("lookback_years", 10)

        series_codes = {
            "btc_price": "BTC_PRICE",
            "btc_supply": "BTC_SUPPLY",
            "btc_issuance": "BTC_ANNUAL_ISSUANCE",
            "defi_tvl": "DEFI_TVL_USD",
            "defi_revenue": "DEFI_PROTOCOL_REVENUE",
            "stablecoin_mcap": "STABLECOIN_MCAP",
            "stablecoin_peg": "USDT_PRICE",
            "bank_deposits": "BANK_DEPOSITS_TOTAL",
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

        results: dict = {"lookback_years": lookback}
        has_any_data = False

        # --- 1. Bitcoin Stock-to-Flow ---
        if data.get("btc_price") and data.get("btc_supply") and data.get("btc_issuance"):
            has_any_data = True
            common_btc = sorted(
                set(data["btc_price"]) & set(data["btc_supply"]) & set(data["btc_issuance"])
            )
            if len(common_btc) >= 10:
                price = np.array([data["btc_price"][d] for d in common_btc])
                supply = np.array([data["btc_supply"][d] for d in common_btc])
                issuance = np.array([data["btc_issuance"][d] for d in common_btc])

                # Stock-to-flow ratio
                s2f = supply / np.maximum(issuance, 1.0)
                ln_price = np.log(np.maximum(price, 1e-10))
                ln_s2f = np.log(np.maximum(s2f, 1e-10))

                # OLS: ln(P) = a + b * ln(S2F) + e
                X_s2f = np.column_stack([np.ones(len(common_btc)), ln_s2f])
                beta_s2f = np.linalg.lstsq(X_s2f, ln_price, rcond=None)[0]
                fitted_s2f = X_s2f @ beta_s2f
                resid_s2f = ln_price - fitted_s2f

                sst = float(np.sum((ln_price - np.mean(ln_price)) ** 2))
                sse = float(np.sum(resid_s2f ** 2))
                r2_s2f = 1.0 - sse / sst if sst > 0 else 0.0

                # Current model-implied price
                implied_price = float(np.exp(fitted_s2f[-1]))
                actual_price = float(price[-1])
                deviation_pct = (actual_price - implied_price) / implied_price * 100

                # Bubble detection: residual z-score
                resid_z = (
                    (resid_s2f[-1] - np.mean(resid_s2f)) / np.std(resid_s2f, ddof=1)
                    if np.std(resid_s2f, ddof=1) > 1e-10 else 0.0
                )

                # Volatility (annualized log returns)
                log_ret = np.diff(ln_price)
                vol_annual = float(np.std(log_ret, ddof=1) * np.sqrt(252))

                results["stock_to_flow"] = {
                    "s2f_latest": round(float(s2f[-1]), 1),
                    "s2f_elasticity": round(float(beta_s2f[1]), 3),
                    "r_squared": round(r2_s2f, 4),
                    "implied_price": round(implied_price, 0),
                    "actual_price": round(actual_price, 0),
                    "deviation_pct": round(deviation_pct, 1),
                    "residual_z_score": round(float(resid_z), 2),
                    "bubble_signal": float(resid_z) > 2.0,
                    "crash_signal": float(resid_z) < -2.0,
                    "annualized_volatility": round(vol_annual, 3),
                    "n_obs": len(common_btc),
                }
            else:
                results["stock_to_flow"] = {"note": "insufficient BTC data"}
        else:
            results["stock_to_flow"] = {"note": "BTC data unavailable"}

        # --- 2. DeFi Yield Analysis ---
        if data.get("defi_tvl"):
            has_any_data = True
            tvl_dates = sorted(data["defi_tvl"])
            tvl = np.array([data["defi_tvl"][d] for d in tvl_dates])

            if len(tvl) >= 5:
                tvl_growth = np.diff(tvl) / np.maximum(tvl[:-1], 1e-10) * 100

                # Sustainable yield proxy
                if data.get("defi_revenue"):
                    rev_dates = sorted(set(data["defi_revenue"]) & set(tvl_dates))
                    if len(rev_dates) >= 5:
                        rev = np.array([data["defi_revenue"][d] for d in rev_dates])
                        tvl_matched = np.array([data["defi_tvl"][d] for d in rev_dates])
                        sustainable_yield = rev / np.maximum(tvl_matched, 1e-10) * 100 * 4  # annualized
                        results["defi_yield"] = {
                            "tvl_latest_bn": round(float(tvl[-1]) / 1e9, 2),
                            "tvl_growth_latest_pct": round(float(tvl_growth[-1]), 2),
                            "sustainable_yield_pct": round(float(sustainable_yield[-1]), 2),
                            "sustainable_yield_mean": round(float(np.mean(sustainable_yield)), 2),
                        }
                    else:
                        results["defi_yield"] = {
                            "tvl_latest_bn": round(float(tvl[-1]) / 1e9, 2),
                            "tvl_growth_latest_pct": round(float(tvl_growth[-1]), 2),
                            "note": "revenue data insufficient for yield calc",
                        }
                else:
                    results["defi_yield"] = {
                        "tvl_latest_bn": round(float(tvl[-1]) / 1e9, 2),
                        "tvl_growth_latest_pct": round(float(tvl_growth[-1]), 2),
                    }
            else:
                results["defi_yield"] = {"note": "insufficient TVL data"}
        else:
            results["defi_yield"] = {"note": "DeFi data unavailable"}

        # --- 3. Stablecoin Depegging Risk ---
        if data.get("stablecoin_peg"):
            has_any_data = True
            peg_dates = sorted(data["stablecoin_peg"])
            peg_price = np.array([data["stablecoin_peg"][d] for d in peg_dates])

            if len(peg_price) >= 5:
                deviation = np.abs(peg_price - 1.0)
                max_deviation = float(np.max(deviation))
                current_deviation = float(deviation[-1])

                # Depeg frequency: how often > 0.5% off peg
                depeg_freq = float(np.mean(deviation > 0.005))

                # Depeg duration: longest consecutive breach
                breached = deviation > 0.005
                max_run = 0
                current_run = 0
                for b in breached:
                    if b:
                        current_run += 1
                        max_run = max(max_run, current_run)
                    else:
                        current_run = 0

                # Trend in deviations (worsening?)
                if len(deviation) > 10:
                    dev_trend = float(np.polyfit(np.arange(len(deviation)), deviation, 1)[0])
                else:
                    dev_trend = 0.0

                results["stablecoin_risk"] = {
                    "current_deviation": round(current_deviation, 5),
                    "max_deviation": round(max_deviation, 5),
                    "depeg_frequency": round(depeg_freq, 3),
                    "max_consecutive_depeg": max_run,
                    "deviation_trend": round(dev_trend, 6),
                    "risk_level": (
                        "high" if current_deviation > 0.01 or depeg_freq > 0.1
                        else "moderate" if current_deviation > 0.005 or depeg_freq > 0.05
                        else "low"
                    ),
                }

                if data.get("stablecoin_mcap"):
                    mcap_dates = sorted(data["stablecoin_mcap"])
                    mcap = np.array([data["stablecoin_mcap"][d] for d in mcap_dates])
                    if len(mcap) >= 3:
                        mcap_change = np.diff(mcap) / np.maximum(mcap[:-1], 1e-10) * 100
                        results["stablecoin_risk"]["mcap_latest_bn"] = round(
                            float(mcap[-1]) / 1e9, 2
                        )
                        results["stablecoin_risk"]["mcap_change_pct"] = round(
                            float(mcap_change[-1]), 2
                        )
            else:
                results["stablecoin_risk"] = {"note": "insufficient peg data"}
        else:
            results["stablecoin_risk"] = {"note": "stablecoin data unavailable"}

        # --- 4. CBDC Impact on Banking Deposits ---
        if data.get("bank_deposits"):
            has_any_data = True
            dep_dates = sorted(data["bank_deposits"])
            deposits = np.array([data["bank_deposits"][d] for d in dep_dates])

            if len(deposits) >= 8:
                dep_growth = np.diff(deposits) / np.maximum(deposits[:-1], 1e-10) * 100

                # Structural change in deposit growth (proxy for CBDC/crypto substitution)
                mid = len(dep_growth) // 2
                if mid > 2:
                    pre_growth = float(np.mean(dep_growth[:mid]))
                    post_growth = float(np.mean(dep_growth[mid:]))
                    # Welch's t-test for difference in means
                    if len(dep_growth[:mid]) > 2 and len(dep_growth[mid:]) > 2:
                        t_stat, p_val = sp_stats.ttest_ind(
                            dep_growth[:mid], dep_growth[mid:], equal_var=False
                        )
                    else:
                        t_stat, p_val = 0.0, 1.0
                else:
                    pre_growth = post_growth = 0.0
                    t_stat = p_val = 0.0

                # Disintermediation scenario: if deposit growth declines by X%,
                # estimate lending capacity impact (simple multiplier)
                deposit_multiplier = 7.0  # approximate money multiplier
                if post_growth < pre_growth and pre_growth > 0:
                    lending_impact_pct = (pre_growth - post_growth) * deposit_multiplier
                else:
                    lending_impact_pct = 0.0

                results["cbdc_impact"] = {
                    "deposit_growth_pre": round(pre_growth, 3),
                    "deposit_growth_post": round(post_growth, 3),
                    "growth_change": round(post_growth - pre_growth, 3),
                    "t_statistic": round(float(t_stat), 3),
                    "p_value": round(float(p_val), 4),
                    "significant_decline": float(p_val) < 0.05 and post_growth < pre_growth,
                    "lending_impact_pct": round(lending_impact_pct, 2),
                    "disintermediation_risk": (
                        "high" if post_growth < 0
                        else "moderate" if post_growth < pre_growth * 0.5
                        else "low"
                    ),
                }
            else:
                results["cbdc_impact"] = {"note": "insufficient deposit data"}
        else:
            results["cbdc_impact"] = {"note": "deposit data unavailable"}

        if not has_any_data:
            return {"score": 50.0, "results": {"error": "no crypto/monetary data available"}}

        # --- Score ---
        # Bubble + depeg risk + disintermediation -> stress
        bubble_penalty = 0.0
        s2f = results.get("stock_to_flow", {})
        if isinstance(s2f, dict):
            if s2f.get("bubble_signal"):
                bubble_penalty = 20.0
            elif s2f.get("crash_signal"):
                bubble_penalty = 25.0
            vol = s2f.get("annualized_volatility", 0)
            if isinstance(vol, (int, float)):
                bubble_penalty += min(vol * 15, 20)

        depeg_penalty = 0.0
        sr = results.get("stablecoin_risk", {})
        if isinstance(sr, dict):
            if sr.get("risk_level") == "high":
                depeg_penalty = 25.0
            elif sr.get("risk_level") == "moderate":
                depeg_penalty = 10.0

        disint_penalty = 0.0
        cbdc = results.get("cbdc_impact", {})
        if isinstance(cbdc, dict):
            if cbdc.get("disintermediation_risk") == "high":
                disint_penalty = 20.0
            elif cbdc.get("disintermediation_risk") == "moderate":
                disint_penalty = 10.0

        score = np.clip(bubble_penalty + depeg_penalty + disint_penalty + 10, 0, 100)

        return {"score": round(float(score), 1), "results": results}
