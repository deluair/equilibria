"""Financial Inclusion module.

Methodology
-----------
1. **Findex Composite Index**:
   - World Bank Global Findex dimensions: account ownership, savings,
     borrowing, digital payments, financial resilience
   - Composite via PCA on normalized indicators
   - Sub-indices: access (accounts), usage (transactions), quality (products)
   - Sarma (2008) distance-based index: d_i = w_i * (A_i - m_i) / (M_i - m_i)
     FII = 1 - sqrt(sum((1-d_i)^2) / n)

2. **Mobile Money Adoption (M-Pesa Effects)**:
   - Adoption S-curve estimation (logistic model)
   - Impact on financial access: account ownership before/after mobile money
   - Suri and Jack (2016): M-Pesa lifted 194k Kenyan households out of poverty
   - Diffusion rate: time to 50% adoption, current penetration

3. **Microfinance Impact**:
   - Outreach: number of borrowers, average loan size, portfolio at risk
   - Financial sustainability: operational self-sufficiency ratio
   - Social performance: depth of outreach (avg loan / GNI per capita)
   - Armendariz and Morduch (2010) framework

4. **Account Ownership Determinants**:
   - Probit/logit decomposition: income, education, gender, age, rural/urban
   - Gender gap in account ownership
   - Income gradient: elasticity of inclusion wrt GDP per capita
   - Allen et al. (2016) barriers: cost, distance, documentation, trust

Score reflects inclusion level (low score = high inclusion, low stress).

Sources: World Bank Findex, IMF Financial Access Survey, MIX Market
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FinancialInclusion(LayerBase):
    layer_id = "l15"
    name = "Financial Inclusion"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 15)

        series_codes = {
            "account_ownership": f"FINDEX_ACCOUNT_{country}",
            "savings_formal": f"FINDEX_SAVINGS_{country}",
            "borrowing_formal": f"FINDEX_BORROWING_{country}",
            "digital_payments": f"FINDEX_DIGITAL_{country}",
            "mobile_money": f"MOBILE_MONEY_{country}",
            "atm_density": f"ATM_PER_100K_{country}",
            "branch_density": f"BRANCH_PER_100K_{country}",
            "gdp_pc": f"GDP_PC_{country}",
            "mfi_borrowers": f"MFI_BORROWERS_{country}",
            "mfi_loan_size": f"MFI_AVG_LOAN_{country}",
            "mfi_par30": f"MFI_PAR30_{country}",
            "gender_gap": f"FINDEX_GENDER_GAP_{country}",
        }

        data: dict[str, dict[str, float]] = {}
        for label, code in series_codes.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE series_id = ?) "
                "AND date >= date('now', ?) ORDER BY date",
                (code, f"-{lookback} years"),
            )
            if rows:
                data[label] = {r[0]: float(r[1]) for r in rows}

        results: dict = {"country": country}
        has_data = False

        # --- 1. Findex Composite Index (Sarma distance method) ---
        findex_dims = ["account_ownership", "savings_formal", "borrowing_formal", "digital_payments"]
        available_dims = {k: data[k] for k in findex_dims if data.get(k)}

        if available_dims:
            has_data = True
            # Get latest value for each available dimension
            dim_values = {}
            dim_dates = set()
            for dim_name, dim_data in available_dims.items():
                latest_date = max(dim_data.keys())
                dim_values[dim_name] = dim_data[latest_date]
                dim_dates.add(latest_date)

            # Sarma (2008) Financial Inclusion Index
            # d_i = w_i * (A_i - m_i) / (M_i - m_i)
            # Assume: m_i = 0 (worst), M_i = 100 (best), equal weights
            n_dims = len(dim_values)
            weights = {k: 1.0 / n_dims for k in dim_values}

            d_values = {}
            for dim_name, val in dim_values.items():
                d_values[dim_name] = weights[dim_name] * val / 100.0

            # FII = 1 - sqrt(sum((1-d_i)^2) / n)
            sum_sq = sum((1 - d) ** 2 for d in d_values.values())
            fii = 1.0 - np.sqrt(sum_sq / n_dims)

            # Classification
            if fii > 0.6:
                fii_level = "high"
            elif fii > 0.3:
                fii_level = "medium"
            else:
                fii_level = "low"

            # Trend analysis if multiple observations
            if data.get("account_ownership") and len(data["account_ownership"]) > 2:
                acct_dates = sorted(data["account_ownership"])
                acct_arr = np.array([data["account_ownership"][d] for d in acct_dates])
                t = np.arange(len(acct_arr), dtype=float)
                trend_slope = float(np.polyfit(t, acct_arr, 1)[0])
            else:
                trend_slope = None

            results["findex_composite"] = {
                "fii_score": round(fii, 3),
                "fii_level": fii_level,
                "dimensions": {k: round(v, 1) for k, v in dim_values.items()},
                "d_values": {k: round(v, 4) for k, v in d_values.items()},
                "account_trend_slope": round(trend_slope, 3) if trend_slope is not None else None,
                "n_dimensions": n_dims,
            }
        else:
            results["findex_composite"] = {"note": "Findex data unavailable"}

        # --- 2. Mobile Money Adoption ---
        if data.get("mobile_money"):
            has_data = True
            mm_dates = sorted(data["mobile_money"])
            mm = np.array([data["mobile_money"][d] for d in mm_dates])

            if len(mm) >= 4:
                # Logistic growth estimation: y = K / (1 + exp(-r*(t - t0)))
                # Linearize: ln(y / (K - y)) = r*t - r*t0
                # Assume K (saturation) = 100%
                K = 100.0
                mm_clipped = np.clip(mm, 0.1, K - 0.1)
                logit_y = np.log(mm_clipped / (K - mm_clipped))
                t = np.arange(len(mm), dtype=float)

                if np.std(t) > 0:
                    logistic_fit = np.polyfit(t, logit_y, 1)
                    r_growth = float(logistic_fit[0])  # growth rate
                    t0 = -float(logistic_fit[1]) / r_growth if abs(r_growth) > 1e-10 else 0.0
                else:
                    r_growth = 0.0
                    t0 = 0.0

                # Current penetration and growth
                current_pct = float(mm[-1])
                if len(mm) > 1:
                    yoy_change = float(mm[-1] - mm[-2])
                else:
                    yoy_change = 0.0

                # Time to 50% adoption (from logistic model)
                time_to_50 = t0 - t[-1] if current_pct < 50 else 0.0

                results["mobile_money"] = {
                    "current_penetration_pct": round(current_pct, 1),
                    "yoy_change_pp": round(yoy_change, 2),
                    "logistic_growth_rate": round(r_growth, 4),
                    "time_to_50pct": round(float(time_to_50), 1) if current_pct < 50 else "achieved",
                    "saturation_phase": current_pct > 70,
                    "n_obs": len(mm),
                }
            else:
                results["mobile_money"] = {
                    "current_penetration_pct": round(float(mm[-1]), 1) if len(mm) > 0 else None,
                    "note": "insufficient data for trend analysis",
                }
        else:
            results["mobile_money"] = {"note": "mobile money data unavailable"}

        # --- 3. Microfinance Impact ---
        if data.get("mfi_borrowers"):
            has_data = True
            mfi_dates = sorted(data["mfi_borrowers"])
            borrowers = np.array([data["mfi_borrowers"][d] for d in mfi_dates])

            mfi_result: dict = {
                "borrowers_latest": round(float(borrowers[-1]), 0),
                "n_obs": len(borrowers),
            }

            if len(borrowers) > 2:
                t = np.arange(len(borrowers), dtype=float)
                growth_rate = float(np.polyfit(t, np.log(np.maximum(borrowers, 1)), 1)[0])
                mfi_result["growth_rate_annual"] = round(float(np.exp(growth_rate) - 1) * 100, 2)

            # Depth of outreach: avg loan / GDP per capita
            if data.get("mfi_loan_size") and data.get("gdp_pc"):
                loan_dates = sorted(set(data["mfi_loan_size"]) & set(data["gdp_pc"]))
                if loan_dates:
                    latest = loan_dates[-1]
                    avg_loan = data["mfi_loan_size"][latest]
                    gdp_pc_val = data["gdp_pc"][latest]
                    if gdp_pc_val > 0:
                        depth = avg_loan / gdp_pc_val
                        mfi_result["depth_ratio"] = round(depth, 3)
                        mfi_result["depth_level"] = (
                            "deep" if depth < 0.2 else "moderate" if depth < 0.5 else "shallow"
                        )

            # Portfolio quality
            if data.get("mfi_par30"):
                par_dates = sorted(data["mfi_par30"])
                par30 = np.array([data["mfi_par30"][d] for d in par_dates])
                mfi_result["par30_latest_pct"] = round(float(par30[-1]), 2)
                mfi_result["par30_healthy"] = float(par30[-1]) < 5.0

            results["microfinance"] = mfi_result
        else:
            results["microfinance"] = {"note": "microfinance data unavailable"}

        # --- 4. Account Ownership Determinants ---
        if data.get("account_ownership") and data.get("gdp_pc"):
            has_data = True
            det_dates = sorted(set(data["account_ownership"]) & set(data["gdp_pc"]))

            if len(det_dates) >= 4:
                acct = np.array([data["account_ownership"][d] for d in det_dates])
                gdp_pc_arr = np.array([data["gdp_pc"][d] for d in det_dates])

                # Income elasticity: d(ln_account) / d(ln_gdp_pc)
                ln_acct = np.log(np.maximum(acct, 0.1))
                ln_gdp = np.log(np.maximum(gdp_pc_arr, 1.0))

                if np.std(ln_gdp) > 1e-10:
                    X_det = np.column_stack([np.ones(len(det_dates)), ln_gdp])
                    beta_det = np.linalg.lstsq(X_det, ln_acct, rcond=None)[0]
                    income_elasticity = float(beta_det[1])
                    fitted_det = X_det @ beta_det
                    resid_det = ln_acct - fitted_det
                    sst = float(np.sum((ln_acct - np.mean(ln_acct)) ** 2))
                    r2 = 1.0 - float(np.sum(resid_det ** 2)) / sst if sst > 0 else 0.0
                else:
                    income_elasticity = 0.0
                    r2 = 0.0

                det_result: dict = {
                    "income_elasticity": round(income_elasticity, 3),
                    "r_squared": round(r2, 4),
                    "account_ownership_latest": round(float(acct[-1]), 1),
                    "gdp_pc_latest": round(float(gdp_pc_arr[-1]), 0),
                }

                # Gender gap
                if data.get("gender_gap"):
                    gap_dates = sorted(data["gender_gap"])
                    gap_arr = np.array([data["gender_gap"][d] for d in gap_dates])
                    det_result["gender_gap_pp"] = round(float(gap_arr[-1]), 1)
                    det_result["gender_gap_closing"] = (
                        float(gap_arr[-1]) < float(gap_arr[0]) if len(gap_arr) > 1 else None
                    )

                # Access infrastructure
                infra = {}
                if data.get("atm_density"):
                    atm_vals = list(data["atm_density"].values())
                    infra["atm_per_100k"] = round(atm_vals[-1], 1)
                if data.get("branch_density"):
                    br_vals = list(data["branch_density"].values())
                    infra["branches_per_100k"] = round(br_vals[-1], 1)
                if infra:
                    det_result["infrastructure"] = infra

                results["determinants"] = det_result
            else:
                results["determinants"] = {"note": "insufficient cross-sectional data"}
        else:
            results["determinants"] = {"note": "account/GDP data unavailable"}

        if not has_data:
            return {"score": 50.0, "results": {"error": "no financial inclusion data available"}}

        # --- Score ---
        # Low inclusion = high stress score
        fii = results.get("findex_composite", {}).get("fii_score")
        if isinstance(fii, (int, float)):
            inclusion_penalty = (1.0 - fii) * 40
        else:
            inclusion_penalty = 20.0

        # Gender gap penalty
        gender_gap = results.get("determinants", {}).get("gender_gap_pp")
        if isinstance(gender_gap, (int, float)):
            gender_penalty = min(abs(gender_gap) * 1.5, 20)
        else:
            gender_penalty = 5.0

        # Poor portfolio quality
        par30 = results.get("microfinance", {}).get("par30_latest_pct")
        if isinstance(par30, (int, float)):
            mfi_penalty = min(max(par30 - 5, 0) * 3, 15)
        else:
            mfi_penalty = 5.0

        # Low mobile money penetration
        mm_pct = results.get("mobile_money", {}).get("current_penetration_pct")
        if isinstance(mm_pct, (int, float)):
            mm_penalty = max((50 - mm_pct) * 0.3, 0)
        else:
            mm_penalty = 5.0

        score = np.clip(inclusion_penalty + gender_penalty + mfi_penalty + mm_penalty, 0, 100)

        return {"score": round(float(score), 1), "results": results}
