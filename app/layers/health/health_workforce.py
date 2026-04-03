"""Health workforce: density norms, training pipeline, brain drain, task-shifting.

Evaluates physician and nurse density against WHO thresholds. Models training
pipeline adequacy (cohort flow from medical schools to practice). Estimates
economic cost of health worker brain drain. Computes cost savings from
evidence-based task-shifting (nurse practitioners, community health workers).

Key references:
    WHO (2006). World Health Report: Working Together for Health. Geneva.
    Chen, L. et al. (2004). Human resources for health: overcoming the crisis.
        Lancet, 364(9449), 1984-1990.
    Naicker, S. et al. (2009). Shortage of healthcare workers in developing
        countries. Kidney International Supplements, 74(113), S102-S107.
    Lewin, S. et al. (2010). Lay health workers in primary and community health
        care for maternal and child health. Cochrane Database Syst Rev.
    Dovlo, D. (2005). Wastage in the health workforce: some perspectives from
        African countries. Human Resources for Health, 3, 6.
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class HealthWorkforce(LayerBase):
    layer_id = "l8"
    name = "Health Workforce"
    weight = 0.20

    # WHO minimum density thresholds per 1,000 population
    WHO_PHYSICIAN_MIN = 1.0        # minimum threshold
    WHO_PHYSICIAN_ADEQUATE = 2.3   # SDG adequate coverage
    WHO_NURSE_MIN = 2.5
    WHO_NURSE_ADEQUATE = 4.5

    # Average annual salary proxies (fraction of GDP per capita)
    PHYSICIAN_SALARY_FACTOR = 8.0   # physicians earn ~8x GDPpc
    NURSE_SALARY_FACTOR = 2.5       # nurses earn ~2.5x GDPpc

    async def compute(self, db, **kwargs) -> dict:
        """Compute health workforce adequacy and brain drain cost.

        Fetches physician density, nurse density, GDP per capita, and population.
        Assesses workforce gaps against WHO thresholds. Estimates training pipeline
        adequacy. Quantifies brain drain fiscal cost. Computes task-shifting ROI.

        Returns dict with score, density_analysis, pipeline, brain_drain, and
        task_shifting.
        """
        country_iso3 = kwargs.get("country_iso3")

        # Physicians per 1,000 population
        physician_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.MED.PHYS.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Nurses and midwives per 1,000 population
        nurse_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.MED.NUMW.P3'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # GDP per capita
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

        # Under-5 mortality (proxy: workforce → health outcomes)
        u5mr_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.DYN.MORT'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not physician_rows and not nurse_rows:
            return {"score": 50, "results": {"error": "no health workforce data"}}

        def _index(rows):
            out: dict[str, dict[str, float]] = {}
            for r in rows:
                out.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]
            return out

        physician_data = _index(physician_rows) if physician_rows else {}
        nurse_data = _index(nurse_rows) if nurse_rows else {}
        gdppc_data = _index(gdppc_rows) if gdppc_rows else {}
        pop_data = _index(pop_rows) if pop_rows else {}
        u5mr_data = _index(u5mr_rows) if u5mr_rows else {}

        density_analysis = None
        pipeline = None
        brain_drain = None
        task_shifting = None

        target = country_iso3

        # --- Density analysis ---
        phys_ts = physician_data.get(target, {}) if target else {}
        nurse_ts = nurse_data.get(target, {}) if target else {}

        phys_val = None
        nurse_val = None
        density_yr = None

        if phys_ts:
            latest_phys_yr = sorted(phys_ts.keys())[-1]
            phys_val = phys_ts[latest_phys_yr]
            density_yr = latest_phys_yr

        if nurse_ts:
            latest_nurse_yr = sorted(nurse_ts.keys())[-1]
            nurse_val = nurse_ts[latest_nurse_yr]
            if density_yr is None:
                density_yr = latest_nurse_yr

        if phys_val is not None or nurse_val is not None:
            phys_gap = max(0.0, self.WHO_PHYSICIAN_ADEQUATE - (phys_val or 0))
            nurse_gap = max(0.0, self.WHO_NURSE_ADEQUATE - (nurse_val or 0))

            # Cross-country percentile
            all_phys = [physician_data[iso][sorted(physician_data[iso].keys())[-1]]
                        for iso in physician_data if physician_data[iso]]
            all_nurses = [nurse_data[iso][sorted(nurse_data[iso].keys())[-1]]
                          for iso in nurse_data if nurse_data[iso]]

            phys_percentile = (
                float(np.mean(np.array(all_phys) <= phys_val)) * 100
                if phys_val is not None and all_phys else None
            )
            nurse_percentile = (
                float(np.mean(np.array(all_nurses) <= nurse_val)) * 100
                if nurse_val is not None and all_nurses else None
            )

            density_analysis = {
                "year": density_yr,
                "physician_per_1000": phys_val,
                "nurse_per_1000": nurse_val,
                "physician_gap_to_who": round(phys_gap, 3),
                "nurse_gap_to_who": round(nurse_gap, 3),
                "physician_meets_minimum": (
                    bool(phys_val >= self.WHO_PHYSICIAN_MIN) if phys_val is not None else None
                ),
                "nurse_meets_minimum": (
                    bool(nurse_val >= self.WHO_NURSE_MIN) if nurse_val is not None else None
                ),
                "physician_percentile_global": (
                    round(phys_percentile, 1) if phys_percentile is not None else None
                ),
                "nurse_percentile_global": (
                    round(nurse_percentile, 1) if nurse_percentile is not None else None
                ),
            }

        # --- Training pipeline adequacy ---
        # Estimate required new graduates per year to close workforce gap
        # and replace attrition (typical attrition 3-5%/year)
        pop_ts = pop_data.get(target, {}) if target else {}
        gdppc_ts = gdppc_data.get(target, {}) if target else {}

        if pop_ts and (phys_val is not None or nurse_val is not None):
            latest_pop_yr = sorted(pop_ts.keys())[-1]
            pop_val = pop_ts[latest_pop_yr]

            current_physicians = (phys_val or 0) * pop_val / 1000
            current_nurses = (nurse_val or 0) * pop_val / 1000

            target_physicians = self.WHO_PHYSICIAN_ADEQUATE * pop_val / 1000
            target_nurses = self.WHO_NURSE_ADEQUATE * pop_val / 1000

            # Assume 10-year horizon to close gap
            gap_years = 10
            attrition_rate = 0.04  # 4% annual attrition
            annual_attrition_phys = current_physicians * attrition_rate
            annual_attrition_nurses = current_nurses * attrition_rate

            # Annual new graduates needed = attrition + gap/10
            phys_grads_needed = annual_attrition_phys + max(
                0, (target_physicians - current_physicians) / gap_years
            )
            nurse_grads_needed = annual_attrition_nurses + max(
                0, (target_nurses - current_nurses) / gap_years
            )

            # Training cost: medical school ~8 years, nursing ~4 years
            physician_training_cost = 60000 * (gdppc_ts.get(latest_pop_yr, 5000) / 5000)
            nurse_training_cost = 15000 * (gdppc_ts.get(latest_pop_yr, 5000) / 5000)

            annual_training_investment = (
                phys_grads_needed * physician_training_cost
                + nurse_grads_needed * nurse_training_cost
            )

            pipeline = {
                "current_physicians": round(current_physicians, 0),
                "current_nurses": round(current_nurses, 0),
                "target_physicians": round(target_physicians, 0),
                "target_nurses": round(target_nurses, 0),
                "annual_phys_grads_needed": round(phys_grads_needed, 0),
                "annual_nurse_grads_needed": round(nurse_grads_needed, 0),
                "annual_training_investment_musd": round(annual_training_investment / 1e6, 2),
                "gap_closure_years": gap_years,
            }

            # --- Brain drain cost estimation ---
            # Dovlo (2005): 24-50% of doctors from LMICs emigrate
            # Fiscal cost: training investment lost to destination countries
            gdppc_val = gdppc_ts.get(latest_pop_yr, 5000)
            if gdppc_val < 5000:
                emigration_rate = 0.35  # high brain drain in LMICs
            elif gdppc_val < 15000:
                emigration_rate = 0.15
            else:
                emigration_rate = 0.05

            annual_phys_emigration = phys_grads_needed * emigration_rate
            fiscal_cost_of_brain_drain = (
                annual_phys_emigration * physician_training_cost * 2.5  # 2.5x: externality
            )

            # Net transfer to destination countries
            net_transfer_pct_gdp = 0.0
            if gdppc_val and pop_val:
                net_transfer_pct_gdp = fiscal_cost_of_brain_drain / (gdppc_val * pop_val) * 100

            brain_drain = {
                "estimated_emigration_rate_pct": round(emigration_rate * 100, 1),
                "annual_physician_emigration": round(annual_phys_emigration, 0),
                "fiscal_cost_brain_drain_musd": round(fiscal_cost_of_brain_drain / 1e6, 2),
                "net_transfer_pct_gdp": round(net_transfer_pct_gdp, 4),
                "policy_response": (
                    "high_priority" if emigration_rate > 0.25
                    else "medium_priority" if emigration_rate > 0.10
                    else "low_priority"
                ),
            }

            # --- Task-shifting cost savings ---
            # Lewin et al. (2010): CHWs provide 60-80% of primary care at 20-30% of cost
            # Nurse practitioners replace ~40% of GP consultations at 50-60% of cost
            gp_consult_cost = gdppc_val * 0.003          # ~0.3% of GDPpc per visit
            np_consult_cost = gp_consult_cost * 0.55     # nurse practitioner 55% of GP cost
            chw_visit_cost = gp_consult_cost * 0.20      # community health worker

            # Consultations per person per year (typical LIC ~1.5, HIC ~7)
            if gdppc_val < 2000:
                consults_per_capita = 1.5
            elif gdppc_val < 10000:
                consults_per_capita = 3.5
            else:
                consults_per_capita = 6.5

            total_consultations = consults_per_capita * pop_val

            # Task-shifting scenario: 30% of physician visits -> NP, 20% -> CHW
            np_shift_pct = 0.30
            chw_shift_pct = 0.20

            savings_from_np = (
                total_consultations * np_shift_pct * (gp_consult_cost - np_consult_cost)
            )
            savings_from_chw = (
                total_consultations * chw_shift_pct * (gp_consult_cost - chw_visit_cost)
            )
            total_savings = savings_from_np + savings_from_chw
            savings_pct_health_spend = total_savings / (
                gdppc_val * pop_val * 0.05
            ) * 100  # relative to ~5% health spend

            task_shifting = {
                "gp_consult_cost_usd": round(gp_consult_cost, 2),
                "np_consult_cost_usd": round(np_consult_cost, 2),
                "chw_visit_cost_usd": round(chw_visit_cost, 2),
                "total_annual_consultations": round(total_consultations, 0),
                "np_savings_musd": round(savings_from_np / 1e6, 2),
                "chw_savings_musd": round(savings_from_chw / 1e6, 2),
                "total_savings_musd": round(total_savings / 1e6, 2),
                "savings_pct_health_expenditure": round(savings_pct_health_spend, 2),
            }

        # --- Outcome correlation: workforce density vs U5MR ---
        outcome_correlation = None
        phys_vals, u5mr_vals, iso_list = [], [], []
        for iso in set(physician_data.keys()) & set(u5mr_data.keys()):
            p_ts = physician_data[iso]
            u_ts = u5mr_data[iso]
            common = sorted(set(p_ts.keys()) & set(u_ts.keys()))
            if common:
                yr = common[-1]
                if p_ts[yr] is not None and u_ts[yr] is not None:
                    phys_vals.append(float(p_ts[yr]))
                    u5mr_vals.append(float(u_ts[yr]))
                    iso_list.append(iso)

        if len(phys_vals) >= 20:
            p_arr = np.array(phys_vals)
            u_arr = np.array(u5mr_vals)
            sl, inter, r, _, pval = linregress(p_arr, u_arr)
            outcome_correlation = {
                "n_countries": len(phys_vals),
                "slope": round(float(sl), 4),
                "intercept": round(float(inter), 2),
                "r_squared": round(float(r) ** 2, 4),
                "p_value": round(float(pval), 4),
                "interpretation": "more physicians -> lower U5MR" if sl < 0 else "inconclusive",
            }

        # --- Score ---
        score = 30.0

        if density_analysis:
            phys_gap = density_analysis["physician_gap_to_who"]
            nurse_gap = density_analysis["nurse_gap_to_who"]
            combined_gap = phys_gap + nurse_gap * 0.5
            if combined_gap > 3:
                score += 35
            elif combined_gap > 1.5:
                score += 20
            elif combined_gap > 0.5:
                score += 10

        if brain_drain:
            if brain_drain["estimated_emigration_rate_pct"] > 25:
                score += 20
            elif brain_drain["estimated_emigration_rate_pct"] > 10:
                score += 10

        if pipeline and pipeline.get("annual_training_investment_musd", 0) == 0:
            score += 5

        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": target,
                "density_analysis": density_analysis,
                "training_pipeline": pipeline,
                "brain_drain": brain_drain,
                "task_shifting": task_shifting,
                "outcome_correlation": outcome_correlation,
            },
        }
