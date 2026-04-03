"""Dollarization: currency substitution, seigniorage loss, dedollarization, autonomy.

Methodology
-----------
1. **Currency Substitution Measurement**:
   Calvo-Vegh (1992) currency substitution model:
   Dollarization index = FX deposits / (FX deposits + domestic currency deposits)
   Asset dollarization: FX assets / total financial system assets.
   Liability dollarization: FX loans / total loans (financial fragility indicator).
   Original sin (Eichengreen-Hausmann 1999): inability to borrow abroad in own currency.
   OSIN = 1 - (securities issued in own currency / total international securities)

2. **Dollarization Costs: Seigniorage Loss**:
   Seigniorage = delta(M) / P = growth rate of monetary base * real money balances
   Under full dollarization, seigniorage accrues to issuing country (US).
   Annual seigniorage loss = inflation_rate * (dollarized_money_stock / GDP)
   Cumulative PV loss = integral over time horizon of seigniorage foregone.
   Ize-Levy-Yeyati (2003) minimum variance portfolio approach to dollarization.

3. **Dedollarization Strategies**:
   Bolivia, Peru, Poland approaches: incentive-based vs. regulatory.
   Levy-Yeyati (2006) taxonomy:
     - Market-based: inflation reduction, indexed instruments
     - Regulatory: reserve requirements on FX deposits, FX loan restrictions
     - Force: capital controls, conversion mandates
   Effectiveness score: change in dollarization index per percentage point of policy intensity.
   Success criterion: sustained 5pp decline in dollarization over 5 years.

4. **Monetary Policy Autonomy Index**:
   Mundell-Fleming trilemma: impossible trinity (fixed rate + open capital + monetary autonomy).
   Autonomy index (Aizenman-Chinn-Ito 2008):
     rho = 1 - 0.5 * |delta(i_dom) - delta(i_anchor)| / (|delta(i_dom)| + |delta(i_anchor)|)
   High rho -> follows anchor country -> low autonomy.
   Interest rate pass-through from anchor to domestic rate (OLS coefficient).

References:
    Calvo, G.A. & Vegh, C.A. (1992). Currency Substitution in Developing Countries:
        An Introduction. IMF Working Paper WP/92/40.
    Eichengreen, B. & Hausmann, R. (1999). Exchange Rates and Financial Fragility.
        NBER Working Paper 7418.
    Ize, A. & Levy-Yeyati, E. (2003). Financial Dollarization. JIE 59(2): 323-347.
    Aizenman, J., Chinn, M. & Ito, H. (2008). Assessing the Emerging Global
        Financial Architecture. NBER Working Paper 14053.

Score: high dollarization + low autonomy + high seigniorage loss -> STRESS.
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class Dollarization(LayerBase):
    layer_id = "l15"
    name = "Dollarization"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 20)
        anchor_country = kwargs.get("anchor_country", "USA")
        time_horizon = kwargs.get("seigniorage_horizon_years", 10)

        series_map = {
            "fx_deposit_share": f"FX_DEPOSIT_SHARE_{country}",
            "fx_loan_share": f"FX_LOAN_SHARE_{country}",
            "monetary_base_gdp": f"MONETARY_BASE_GDP_{country}",
            "inflation": f"INFLATION_{country}",
            "policy_rate": f"POLICY_RATE_{country}",
            "anchor_rate": f"POLICY_RATE_{anchor_country}",
            "dedollar_index": f"DEDOLLAR_POLICY_INDEX_{country}",
            "osin": f"ORIGINAL_SIN_INDEX_{country}",
        }

        data: dict[str, dict[str, float]] = {}
        for label, code in series_map.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE series_id = ?) "
                "AND date >= date('now', ?) ORDER BY date",
                (code, f"-{lookback} years"),
            )
            if rows:
                data[label] = {r[0]: float(r[1]) for r in rows}

        results: dict = {"country": country, "anchor_country": anchor_country}
        has_any = False

        # --- 1. Currency Substitution ---
        subst: dict = {}
        if data.get("fx_deposit_share"):
            has_any = True
            dep_dates = sorted(data["fx_deposit_share"])
            dep = np.array([data["fx_deposit_share"][d] for d in dep_dates])
            subst["fx_deposit_share_latest"] = round(float(dep[-1]), 4)
            subst["fx_deposit_share_mean"] = round(float(np.mean(dep)), 4)
            subst["dollarization_level"] = (
                "high" if dep[-1] > 0.50
                else "moderate" if dep[-1] > 0.25
                else "low"
            )
            subst["trend"] = round(float(np.polyfit(np.arange(len(dep)), dep, 1)[0]), 6)
            subst["n_obs"] = len(dep)

        if data.get("fx_loan_share"):
            has_any = True
            ln_dates = sorted(data["fx_loan_share"])
            ln = np.array([data["fx_loan_share"][d] for d in ln_dates])
            subst["fx_loan_share_latest"] = round(float(ln[-1]), 4)
            subst["liability_dollarization_high"] = bool(ln[-1] > 0.40)

        if data.get("osin"):
            has_any = True
            os_dates = sorted(data["osin"])
            os_v = np.array([data["osin"][d] for d in os_dates])
            subst["original_sin_latest"] = round(float(os_v[-1]), 4)
            subst["original_sin_severe"] = bool(os_v[-1] > 0.75)

        if subst:
            results["currency_substitution"] = subst

        # --- 2. Seigniorage Loss ---
        seigniorage: dict = {}
        if data.get("inflation") and data.get("monetary_base_gdp") and data.get("fx_deposit_share"):
            has_any = True
            common = sorted(
                set(data["inflation"]) & set(data["monetary_base_gdp"]) & set(data["fx_deposit_share"])
            )
            if common:
                inf = np.array([data["inflation"][d] for d in common]) / 100.0
                mb = np.array([data["monetary_base_gdp"][d] for d in common])
                dol = np.array([data["fx_deposit_share"][d] for d in common])

                # Seigniorage lost = inflation * monetary_base_gdp * dollarization_share
                annual_loss = inf * mb * dol
                mean_loss = float(np.mean(annual_loss))
                current_loss = float(annual_loss[-1])

                # PV of cumulative loss over horizon (simple annuity, discount=5%)
                discount_rate = 0.05
                pv_factor = (1.0 - (1.0 + discount_rate) ** (-time_horizon)) / discount_rate
                pv_loss = mean_loss * pv_factor

                seigniorage["annual_loss_pct_gdp"] = round(current_loss * 100, 4)
                seigniorage["mean_annual_loss_pct_gdp"] = round(mean_loss * 100, 4)
                seigniorage["pv_loss_pct_gdp"] = round(pv_loss * 100, 4)
                seigniorage["horizon_years"] = time_horizon
                results["seigniorage"] = seigniorage

        # --- 3. Dedollarization ---
        dedol: dict = {}
        if data.get("dedollar_index") and data.get("fx_deposit_share"):
            has_any = True
            common_d = sorted(set(data["dedollar_index"]) & set(data["fx_deposit_share"]))
            if len(common_d) >= 5:
                policy = np.array([data["dedollar_index"][d] for d in common_d])
                dol_arr = np.array([data["fx_deposit_share"][d] for d in common_d])

                if np.std(policy, ddof=1) > 1e-10:
                    slope, intercept, r, p, se = sp_stats.linregress(policy, dol_arr)
                    dedol["policy_effectiveness"] = round(float(slope), 4)
                    dedol["r_squared"] = round(float(r ** 2), 4)
                    dedol["p_value"] = round(float(p), 4)
                    dedol["significant"] = float(p) < 0.10 and float(slope) < 0

                # Check for sustained 5pp decline
                if len(dol_arr) >= 5:
                    recent_change = float(dol_arr[-1]) - float(dol_arr[-6]) if len(dol_arr) >= 6 else float(dol_arr[-1]) - float(dol_arr[0])
                    dedol["5yr_change_pp"] = round(recent_change * 100, 2)
                    dedol["sustained_decline"] = recent_change < -0.05
                results["dedollarization"] = dedol

        # --- 4. Monetary Policy Autonomy ---
        if data.get("policy_rate") and data.get("anchor_rate"):
            has_any = True
            common_r = sorted(set(data["policy_rate"]) & set(data["anchor_rate"]))
            if len(common_r) >= 8:
                dom = np.array([data["policy_rate"][d] for d in common_r])
                anc = np.array([data["anchor_rate"][d] for d in common_r])

                d_dom = np.diff(dom)
                d_anc = np.diff(anc)

                # Aizenman-Chinn-Ito autonomy index
                denom = np.abs(d_dom) + np.abs(d_anc)
                valid = denom > 1e-10
                if valid.sum() > 2:
                    rho = 1.0 - 0.5 * np.abs(d_dom[valid] - d_anc[valid]) / denom[valid]
                    autonomy_index = float(np.mean(rho))
                else:
                    autonomy_index = None

                # OLS pass-through: d_dom = a + b * d_anc
                if np.std(d_anc, ddof=1) > 1e-10:
                    X = np.column_stack([np.ones(len(d_anc)), d_anc])
                    beta = np.linalg.lstsq(X, d_dom, rcond=None)[0]
                    pass_through = float(beta[1])
                else:
                    pass_through = None

                results["monetary_autonomy"] = {
                    "autonomy_index": round(autonomy_index, 4) if autonomy_index is not None else None,
                    "low_autonomy": autonomy_index < 0.4 if autonomy_index is not None else None,
                    "pass_through_coef": round(pass_through, 4) if pass_through is not None else None,
                    "high_pass_through": pass_through > 0.7 if pass_through is not None else None,
                    "n_obs": len(common_r),
                }

        if not has_any:
            return {"score": 50.0, "results": {"error": "no dollarization data available"}}

        # --- Score ---
        stress = 10.0

        cs = results.get("currency_substitution", {})
        if cs.get("dollarization_level") == "high":
            stress += 25.0
        elif cs.get("dollarization_level") == "moderate":
            stress += 12.0
        if cs.get("liability_dollarization_high"):
            stress += 15.0
        if cs.get("original_sin_severe"):
            stress += 10.0

        sg = results.get("seigniorage", {})
        if sg.get("annual_loss_pct_gdp") is not None:
            stress += min(float(sg["annual_loss_pct_gdp"]) * 2.0, 15.0)

        ma = results.get("monetary_autonomy", {})
        if ma.get("low_autonomy"):
            stress += 15.0
        if ma.get("high_pass_through"):
            stress += 10.0

        score = max(0.0, min(100.0, stress))

        return {"score": round(score, 1), "results": results}
