"""Pension reform economics: PAYGO vs funded transition, actuarial balance,
replacement rate adequacy, and parametric reform simulations.

Key frameworks:

1. PAYGO actuarial balance (Aaron 1966):
   Implicit rate of return = n + g  (population growth + wage growth)
   Funded rate of return = r  (capital market return)
   Samuelson (1958) social optimum: PAYGO preferred if n+g > r.
   Actuarial balance: PV(projected revenues) - PV(projected obligations) over 75yr horizon.

2. Replacement rate adequacy (ILO Social Security Minimum Standard, C102):
   Adequate = gross replacement rate >= 40% for standard beneficiary.
   Net replacement rate adjusts for taxes and means-tested benefits.

3. Transition cost (Kotlikoff 1996, Feldstein 1997):
   Switching from PAYGO to funded requires honoring legacy obligations while
   funding new accounts. Transition debt = PV of accrued PAYGO liabilities.
   Annual transition cost ~ 1-3% of GDP over 30-50 years.

4. Parametric reform levers:
   - Retirement age increase: reduces liability, improves dependency ratio
   - Contribution rate increase: raises revenue, may reduce labor supply
   - Indexation switch (wage->price): reduces real benefit growth
   - Benefit formula tightening: reduces accrual rate

References:
    Aaron, H. (1966). The Social Insurance Paradox. Canadian JE&PS.
    Samuelson, P. (1958). An Exact Consumption-Loan Model. JPE 66(6).
    Feldstein, M. (1997). Transition to a Fully Funded Pension System. NBER.
    Kotlikoff, L. (1996). Privatizing Social Security at Home or Abroad. AER.
    ILO Convention C102 (1952). Social Security Minimum Standards.

Sources: IMF WEO, World Bank pension statistics, ILO ISSA data.
"""

from __future__ import annotations

import json

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class PensionReform(LayerBase):
    layer_id = "l10"
    name = "Pension Reform"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        results: dict = {"country": country}

        # --- Replacement rate adequacy ---
        rr_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('PENSION_REPLACEMENT_RATE', 'SI.POV.PENR')
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        replacement_rate_result = None
        if rr_rows:
            latest = rr_rows[0]
            gross_rr = float(latest["value"])
            meta = json.loads(latest["metadata"]) if latest.get("metadata") else {}
            net_rr = meta.get("net_replacement_rate", gross_rr * 0.85)
            adequacy_threshold = 40.0  # ILO C102 minimum

            replacement_rate_result = {
                "gross_replacement_rate": round(gross_rr, 2),
                "net_replacement_rate": round(float(net_rr), 2),
                "ilo_threshold": adequacy_threshold,
                "adequate": gross_rr >= adequacy_threshold,
                "adequacy_gap": round(max(0.0, adequacy_threshold - gross_rr), 2),
                "date": latest["date"],
            }
        results["replacement_rate"] = replacement_rate_result or {"error": "no replacement rate data"}

        # --- PAYGO actuarial balance ---
        fiscal_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('PENSION_EXPENDITURE_GDP', 'SOC_PENSION_EXP')
            ORDER BY dp.date
            """,
            (country,),
        )

        demographic_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('OLD_AGE_DEPENDENCY_RATIO', 'SP.POP.DPND.OL')
            ORDER BY dp.date
            """,
            (country,),
        )

        actuarial_result = None
        if fiscal_rows and demographic_rows:
            exp_vals = [float(r["value"]) for r in fiscal_rows[-10:]]
            dep_vals = [float(r["value"]) for r in demographic_rows[-10:]]

            if len(exp_vals) >= 5 and len(dep_vals) >= 5:
                # Aaron condition: n+g vs r
                # Proxy population growth from dependency ratio trend
                dep_arr = np.array(dep_vals)
                if len(dep_arr) >= 3:
                    t = np.arange(len(dep_arr))
                    slope_dep, _, _, _, _ = sp_stats.linregress(t, dep_arr)
                    aging_rate = float(slope_dep)  # increase in dependency ratio per year
                else:
                    aging_rate = 0.0

                latest_exp = exp_vals[-1]
                latest_dep = dep_vals[-1]
                exp_trend = np.polyfit(np.arange(len(exp_vals)), exp_vals, 1)[0] if len(exp_vals) >= 3 else 0.0

                # Simplified 30yr forward projection: exp grows with aging
                discount_rate = 0.03
                horizon = 30
                pv_revenue = 0.0
                pv_obligation = 0.0
                exp_path = latest_exp
                rev_rate = latest_exp * 0.9  # typical revenue slightly below expenditure
                for t_yr in range(1, horizon + 1):
                    growth_factor = (1 + aging_rate * 0.02) ** t_yr  # aging drives cost growth
                    pv_obligation += exp_path * growth_factor / (1 + discount_rate) ** t_yr
                    pv_revenue += rev_rate / (1 + discount_rate) ** t_yr

                actuarial_balance = pv_revenue - pv_obligation

                actuarial_result = {
                    "latest_expenditure_pct_gdp": round(latest_exp, 2),
                    "old_age_dependency_ratio": round(latest_dep, 2),
                    "aging_rate_annual": round(aging_rate, 3),
                    "expenditure_trend": round(float(exp_trend), 3),
                    "pv_obligations_30yr": round(pv_obligation, 2),
                    "pv_revenues_30yr": round(pv_revenue, 2),
                    "actuarial_balance_pct_gdp": round(actuarial_balance, 2),
                    "status": "surplus" if actuarial_balance > 0 else "deficit",
                }
        results["actuarial_balance"] = actuarial_result or {"error": "insufficient fiscal/demographic data"}

        # --- Parametric reform simulation ---
        if actuarial_result and replacement_rate_result:
            exp_base = actuarial_result["latest_expenditure_pct_gdp"]
            rr_base = replacement_rate_result["gross_replacement_rate"]
            dep_base = actuarial_result["old_age_dependency_ratio"]

            # Retirement age +2 years: reduces beneficiaries ~5%, improves actuarial balance
            ra_saving = exp_base * 0.05
            # Contribution rate +1pp: raises revenue ~0.8% GDP (depends on covered employment)
            contrib_gain = 0.8
            # Indexation switch wage->price: reduces RR by ~2pp over 10yr
            indexation_rr_cut = 2.0
            # Tighten accrual rate 10%: reduces RR by ~5pp
            accrual_rr_cut = rr_base * 0.10

            reform_result = {
                "retirement_age_plus2": {
                    "expenditure_saving_pct_gdp": round(ra_saving, 2),
                    "replacement_rate_impact": 0.0,
                    "net_balance_improvement": round(ra_saving, 2),
                },
                "contribution_rate_plus1pp": {
                    "revenue_gain_pct_gdp": round(contrib_gain, 2),
                    "replacement_rate_impact": 0.0,
                    "net_balance_improvement": round(contrib_gain, 2),
                },
                "indexation_switch": {
                    "expenditure_saving_pct_gdp": round(exp_base * 0.03, 2),
                    "replacement_rate_impact": round(-indexation_rr_cut, 2),
                    "net_balance_improvement": round(exp_base * 0.03, 2),
                },
                "accrual_rate_tighten_10pct": {
                    "expenditure_saving_pct_gdp": round(exp_base * 0.07, 2),
                    "replacement_rate_impact": round(-accrual_rr_cut, 2),
                    "net_balance_improvement": round(exp_base * 0.07, 2),
                },
            }
            results["parametric_reform_simulations"] = reform_result
        else:
            results["parametric_reform_simulations"] = {"error": "insufficient base data for simulation"}

        # --- Transition cost estimation ---
        transition_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('IMPLICIT_PENSION_DEBT', 'PAYGO_LIABILITY_GDP')
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        if transition_rows:
            meta = json.loads(transition_rows[0]["metadata"]) if transition_rows[0].get("metadata") else {}
            implicit_debt = float(transition_rows[0]["value"])
            transition_years = meta.get("transition_years", 40)
            annual_cost = implicit_debt / transition_years if transition_years > 0 else 0.0
            results["transition_cost"] = {
                "implicit_pension_debt_pct_gdp": round(implicit_debt, 1),
                "amortization_years": int(transition_years),
                "annual_transition_cost_pct_gdp": round(annual_cost, 2),
                "feasibility": "feasible" if annual_cost < 2.0 else "challenging" if annual_cost < 4.0 else "severe",
            }
        else:
            results["transition_cost"] = {"error": "no implicit pension debt data"}

        # --- Score ---
        score = 30.0

        # Replacement rate inadequacy
        if replacement_rate_result and not replacement_rate_result.get("error"):
            gap = replacement_rate_result.get("adequacy_gap", 0)
            score += min(30.0, gap * 0.8)

        # Actuarial deficit
        if actuarial_result and not actuarial_result.get("error"):
            if actuarial_result["status"] == "deficit":
                balance = abs(actuarial_result["actuarial_balance_pct_gdp"])
                score += min(25.0, balance * 2.0)
            dep = actuarial_result.get("old_age_dependency_ratio", 0)
            if dep > 30:
                score += 10.0
            elif dep > 20:
                score += 5.0

        # Transition feasibility
        tc = results.get("transition_cost", {})
        if tc.get("feasibility") == "severe":
            score += 15.0
        elif tc.get("feasibility") == "challenging":
            score += 7.0

        score = float(np.clip(score, 0.0, 100.0))
        return {"score": round(score, 1), "results": results}
