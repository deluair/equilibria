"""Mental health economics: productivity loss, treatment cost-effectiveness, DALY burden.

Estimates the macroeconomic cost of depression and anxiety via presenteeism and
absenteeism. Computes treatment cost-effectiveness using incremental cost-
effectiveness ratios (ICERs). Evaluates workplace intervention ROI following
the Milliman & Work Foundation frameworks. Quantifies the DALY burden of
mental disorders.

Key references:
    Whiteford, H.A. et al. (2013). Global burden of disease attributable to
        mental and substance use disorders. Lancet, 382(9904), 1575-1586.
    Chisholm, D. et al. (2016). Scaling-up treatment of depression and anxiety:
        a global return on investment analysis. Lancet Psychiatry, 3(5), 415-424.
    Lerner, D. & Henke, R.M. (2008). What does research tell us about
        depression, job performance, and work productivity? JOEM, 50(4), 401-410.
    Weehuizen, R. (2008). Mental Capital: The Economic Significance of Mental
        Health. Maastricht University.
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class MentalHealthEconomics(LayerBase):
    layer_id = "l8"
    name = "Mental Health Economics"
    weight = 0.20

    # Disability weights (GBD 2019)
    DW_DEPRESSION_MODERATE = 0.145
    DW_DEPRESSION_SEVERE = 0.396
    DW_ANXIETY_MODERATE = 0.133

    # Presenteeism/absenteeism multipliers (Lerner & Henke 2008)
    PRODUCTIVITY_LOSS_DEPRESSION = 0.35   # 35% productivity loss when ill
    ABSENTEEISM_DAYS_DEPRESSION = 5.6     # extra absent days/year
    PRESENTEEISM_LOSS_ANXIETY = 0.23      # 23% presenteeism loss

    async def compute(self, db, **kwargs) -> dict:
        """Compute mental health economic burden and treatment ROI.

        Fetches GDP, population, mental health expenditure, and suicide rate data.
        Estimates productivity loss from depression/anxiety. Calculates ICERs for
        scaled-up treatment. Estimates DALY burden from mental disorders.

        Returns dict with score, productivity_loss, treatment_cba, daly_burden,
        and workplace_roi.
        """
        country_iso3 = kwargs.get("country_iso3")

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

        # Population
        pop_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.POP.TOTL'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Health expenditure (% of GDP) - proxy for mental health investment capacity
        health_exp_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.XPD.CHEX.GD.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Suicide mortality rate (per 100k) - indicator of untreated mental illness
        suicide_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.STA.SUIC.P5'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Life expectancy
        le_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.DYN.LE00.IN'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not gdppc_rows or not pop_rows:
            return {"score": 50, "results": {"error": "insufficient GDP or population data"}}

        def _index(rows):
            out: dict[str, dict[str, float]] = {}
            for r in rows:
                out.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]
            return out

        gdppc_data = _index(gdppc_rows)
        pop_data = _index(pop_rows)
        health_exp_data = _index(health_exp_rows) if health_exp_rows else {}
        suicide_data = _index(suicide_rows) if suicide_rows else {}
        le_data = _index(le_rows) if le_rows else {}

        # --- Productivity loss from depression and anxiety ---
        productivity_loss = None
        treatment_cba = None
        daly_burden = None
        workplace_roi = None

        target_iso = country_iso3
        gdppc_ts = gdppc_data.get(target_iso, {}) if target_iso else {}
        pop_ts = pop_data.get(target_iso, {}) if target_iso else {}

        if gdppc_ts and pop_ts:
            latest_yr = sorted(set(gdppc_ts.keys()) & set(pop_ts.keys()))[-1] if (
                set(gdppc_ts.keys()) & set(pop_ts.keys())
            ) else None

            if latest_yr:
                gdppc = gdppc_ts[latest_yr]
                pop = pop_ts[latest_yr]
                total_gdp = gdppc * pop

                # WHO/ILO prevalence estimates: depression ~5%, anxiety ~4% adults
                # Active workforce = ~60% of population
                workforce = pop * 0.60
                depression_prev = 0.050
                anxiety_prev = 0.040

                depressed_workers = workforce * depression_prev
                anxious_workers = workforce * anxiety_prev

                # Productivity loss: absenteeism + presenteeism
                working_days = 250
                daily_wage = gdppc / working_days

                absenteeism_cost = (
                    depressed_workers * self.ABSENTEEISM_DAYS_DEPRESSION * daily_wage
                )
                presenteeism_cost_dep = (
                    depressed_workers * working_days * daily_wage * self.PRODUCTIVITY_LOSS_DEPRESSION
                )
                presenteeism_cost_anx = (
                    anxious_workers * working_days * daily_wage * self.PRESENTEEISM_LOSS_ANXIETY
                )

                total_prod_loss = absenteeism_cost + presenteeism_cost_dep + presenteeism_cost_anx
                prod_loss_pct_gdp = total_prod_loss / total_gdp * 100

                productivity_loss = {
                    "year": latest_yr,
                    "gdp_per_capita_usd": float(gdppc),
                    "workforce_size": float(workforce),
                    "depressed_workers": float(depressed_workers),
                    "anxious_workers": float(anxious_workers),
                    "absenteeism_cost_musd": round(absenteeism_cost / 1e6, 2),
                    "presenteeism_cost_musd": round(
                        (presenteeism_cost_dep + presenteeism_cost_anx) / 1e6, 2
                    ),
                    "total_productivity_loss_musd": round(total_prod_loss / 1e6, 2),
                    "productivity_loss_pct_gdp": round(prod_loss_pct_gdp, 3),
                }

                # --- Treatment cost-effectiveness (Chisholm et al. 2016) ---
                # Scaled-up treatment: antidepressants + psychotherapy
                # Cost per treated person (lower-middle income benchmark)
                if gdppc < 2000:
                    tx_cost_per_person = 120.0    # USD/year
                elif gdppc < 10000:
                    tx_cost_per_person = 280.0
                else:
                    tx_cost_per_person = 750.0

                # Treatment efficacy: ~30% response for antidepressants, ~50% for combo
                remission_rate = 0.45
                treated_target = depressed_workers * 0.30  # scale-up to 30% coverage

                treatment_cost = treated_target * tx_cost_per_person
                years_lived_with_disability_averted = (
                    treated_target * remission_rate * self.DW_DEPRESSION_MODERATE
                )

                icer_per_daly = (
                    treatment_cost / years_lived_with_disability_averted
                    if years_lived_with_disability_averted > 0 else None
                )

                # Productivity benefit of treated workers
                productivity_gained = (
                    treated_target * remission_rate * working_days * daily_wage
                    * self.PRODUCTIVITY_LOSS_DEPRESSION
                )

                treatment_bcr = (
                    productivity_gained / treatment_cost if treatment_cost > 0 else None
                )

                treatment_cba = {
                    "tx_cost_per_person_usd": tx_cost_per_person,
                    "scale_up_coverage_pct": 30.0,
                    "treated_persons": round(treated_target, 0),
                    "total_treatment_cost_musd": round(treatment_cost / 1e6, 2),
                    "dalys_averted": round(years_lived_with_disability_averted, 0),
                    "icer_usd_per_daly": round(float(icer_per_daly), 2) if icer_per_daly else None,
                    "productivity_gained_musd": round(productivity_gained / 1e6, 2),
                    "benefit_cost_ratio": round(float(treatment_bcr), 2) if treatment_bcr else None,
                    "cost_effective": bool(icer_per_daly and icer_per_daly < 3 * gdppc),
                }

                # --- DALY burden estimate ---
                le = le_data.get(target_iso, {})
                le_val = le[sorted(le.keys())[-1]] if le else 70.0

                # YLD from mental disorders: prevalence * disability weight * duration
                yld_depression = depressed_workers * self.DW_DEPRESSION_MODERATE
                yld_anxiety = anxious_workers * self.DW_ANXIETY_MODERATE

                # YLL from suicide
                suicide_rate = None
                if target_iso in suicide_data:
                    s_ts = suicide_data[target_iso]
                    if s_ts:
                        suicide_rate = s_ts[sorted(s_ts.keys())[-1]]

                yll_suicide = 0.0
                if suicide_rate is not None:
                    # Avg years lost per suicide death: life expectancy - avg age of suicide (~40)
                    yll_per_suicide_death = max(0.0, le_val - 40.0)
                    suicide_deaths = pop * suicide_rate / 1e5
                    yll_suicide = suicide_deaths * yll_per_suicide_death

                total_daly = yld_depression + yld_anxiety + yll_suicide
                daly_per_1000 = total_daly / pop * 1000

                daly_burden = {
                    "yld_depression": round(yld_depression, 0),
                    "yld_anxiety": round(yld_anxiety, 0),
                    "yll_suicide": round(yll_suicide, 0),
                    "total_daly": round(total_daly, 0),
                    "daly_per_1000_population": round(daly_per_1000, 2),
                    "suicide_rate_per_100k": float(suicide_rate) if suicide_rate else None,
                }

                # --- Workplace intervention ROI ---
                # Evidence-based interventions: EAP, stress management, manager training
                # ROI from work foundation: GBP 6-9 per GBP 1 invested
                eap_cost_per_employee = min(120.0, gdppc * 0.003)
                eap_coverage = workforce * 0.40  # 40% coverage target
                eap_total_cost = eap_coverage * eap_cost_per_employee

                # Benefit: reduced absenteeism (20% reduction) + presenteeism (15%)
                absenteeism_reduction = absenteeism_cost * 0.20
                presenteeism_reduction = presenteeism_cost_dep * 0.15

                eap_total_benefit = absenteeism_reduction + presenteeism_reduction
                eap_roi = (
                    (eap_total_benefit - eap_total_cost) / eap_total_cost
                    if eap_total_cost > 0 else None
                )

                workplace_roi = {
                    "eap_cost_per_employee_usd": round(eap_cost_per_employee, 2),
                    "employees_covered": round(eap_coverage, 0),
                    "total_program_cost_musd": round(eap_total_cost / 1e6, 2),
                    "absenteeism_savings_musd": round(absenteeism_reduction / 1e6, 2),
                    "presenteeism_savings_musd": round(presenteeism_reduction / 1e6, 2),
                    "net_roi_ratio": round(float(eap_roi), 2) if eap_roi else None,
                    "return_per_usd_invested": (
                        round(float(eap_roi) + 1.0, 2) if eap_roi else None
                    ),
                }

        # --- Cross-country mental health investment analysis ---
        health_investment = {}
        for iso in set(gdppc_data.keys()) & set(health_exp_data.keys()):
            gdp_ts = gdppc_data[iso]
            he_ts = health_exp_data[iso]
            common = sorted(set(gdp_ts.keys()) & set(he_ts.keys()))
            if common:
                yr = common[-1]
                health_investment[iso] = {
                    "gdp_per_capita": gdppc_data[iso][yr],
                    "health_exp_pct_gdp": health_exp_data[iso][yr],
                }

        # Mental health spending is ~10-15% of health budget globally (WHO)
        mental_health_proxy = None
        if target_iso and target_iso in health_investment:
            he_pct = health_investment[target_iso]["health_exp_pct_gdp"]
            # WHO recommends >=5% of health budget for mental health
            mh_pct_health = 0.10  # assumed 10% of health spending
            mh_pct_gdp = he_pct * mh_pct_health

            all_he = [v["health_exp_pct_gdp"] for v in health_investment.values()]
            he_arr = np.array(all_he)
            target_he = health_investment[target_iso]["health_exp_pct_gdp"]
            percentile = float(np.mean(he_arr <= target_he)) * 100

            mental_health_proxy = {
                "health_exp_pct_gdp": round(he_pct, 2),
                "est_mental_health_pct_gdp": round(mh_pct_gdp, 3),
                "health_exp_percentile_global": round(percentile, 1),
                "who_recommended_health_pct_gdp": 5.0,
                "below_who_threshold": bool(he_pct < 5.0),
            }

        # --- Score ---
        # Higher score = higher burden / lower preparedness
        score = 30.0

        if productivity_loss:
            pct = productivity_loss["productivity_loss_pct_gdp"]
            if pct > 4:
                score += 25
            elif pct > 2:
                score += 15
            elif pct > 1:
                score += 8

        if daly_burden:
            suicide = daly_burden.get("suicide_rate_per_100k")
            if suicide is not None:
                if suicide > 15:
                    score += 20
                elif suicide > 8:
                    score += 10
                elif suicide > 4:
                    score += 5

        if treatment_cba and treatment_cba.get("benefit_cost_ratio") is not None:
            if treatment_cba["benefit_cost_ratio"] < 1:
                score += 10  # treatment not cost-effective = unmet need

        if mental_health_proxy and mental_health_proxy["below_who_threshold"]:
            score += 10

        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": target_iso,
                "productivity_loss": productivity_loss,
                "treatment_cost_effectiveness": treatment_cba,
                "daly_burden": daly_burden,
                "workplace_intervention_roi": workplace_roi,
                "mental_health_investment": mental_health_proxy,
            },
        }
