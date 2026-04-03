"""Circular economy: material circularity rate, waste-to-resource, EPR, circular jobs.

Estimates the material circularity rate following Ellen MacArthur Foundation
methodology. Models waste-to-resource conversion economics. Evaluates extended
producer responsibility (EPR) scheme effectiveness using market instrument theory.
Quantifies circular economy employment multipliers.

Key references:
    Ellen MacArthur Foundation (2013). Towards the Circular Economy. EMF.
    Kirchherr, J. et al. (2017). Conceptualizing the circular economy: an
        analysis of 114 definitions. Resources, Conservation and Recycling.
    Ghisellini, P. et al. (2016). A review on circular economy: the expected
        transition to a balanced interplay of environmental and economic systems.
        Journal of Cleaner Production, 114, 11-32.
    Murray, A. et al. (2017). The circular economy: An interdisciplinary
        exploration of the concept and application in a global context.
        Journal of Business Ethics, 140(3), 369-380.
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class CircularEconomy(LayerBase):
    layer_id = "l9"
    name = "Circular Economy"
    weight = 0.20

    # EU circular economy benchmark (2023): 11.5% material circularity rate
    EU_CIRCULARITY_BENCHMARK = 11.5
    GLOBAL_AVG_CIRCULARITY = 7.2   # Ellen MacArthur Foundation 2023

    async def compute(self, db, **kwargs) -> dict:
        """Compute circular economy metrics and EPR effectiveness.

        Fetches GDP, population, energy intensity, and material use proxies.
        Estimates circularity rate, waste recovery, EPR effectiveness, and
        circular jobs multiplier.

        Returns dict with score, circularity_rate, waste_economics, epr_assessment,
        and employment_impact.
        """
        country_iso3 = kwargs.get("country_iso3")

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

        # GDP total
        gdp_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.MKTP.KD'
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

        # Energy intensity (proxy for material intensity and resource efficiency)
        energy_int_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'EG.EGY.PRIM.PP.KD'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # CO2 emissions (kt) - proxy for linear economy intensity
        co2_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'EN.ATM.CO2E.KT'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Industry value added (% GDP) - higher industry = more materials use
        industry_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NV.IND.TOTL.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not gdppc_rows and not gdp_rows:
            return {"score": 50, "results": {"error": "no GDP data"}}

        def _index(rows):
            out: dict[str, dict[str, float]] = {}
            for r in rows:
                out.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]
            return out

        gdppc_data = _index(gdppc_rows) if gdppc_rows else {}
        gdp_data = _index(gdp_rows) if gdp_rows else {}
        pop_data = _index(pop_rows) if pop_rows else {}
        energy_int_data = _index(energy_int_rows) if energy_int_rows else {}
        co2_data = _index(co2_rows) if co2_rows else {}
        industry_data = _index(industry_rows) if industry_rows else {}

        circularity_rate = None
        waste_economics = None
        epr_assessment = None
        employment_impact = None

        target = country_iso3
        gdppc_ts = gdppc_data.get(target, {}) if target else {}
        gdp_ts = gdp_data.get(target, {}) if target else {}
        pop_ts = pop_data.get(target, {}) if target else {}
        ei_ts = energy_int_data.get(target, {}) if target else {}
        co2_ts = co2_data.get(target, {}) if target else {}
        industry_ts = industry_data.get(target, {}) if target else {}

        if gdppc_ts and pop_ts:
            latest_yr = sorted(set(gdppc_ts.keys()) & set(pop_ts.keys()))[-1]
            gdppc = gdppc_ts[latest_yr]
            pop = pop_ts[latest_yr]
            gdp_val = gdp_ts.get(latest_yr, gdppc * pop) if gdp_ts else gdppc * pop

            # --- Circularity rate estimation ---
            # EMF methodology: circularity = secondary material flows /
            #                               (secondary + primary material flows)
            # Proxy: income-based circularity estimate + energy intensity adjustment
            # HICs tend toward services (lower material intensity = higher circularity)
            if gdppc > 40000:
                base_circularity = 14.0    # EU-like mature circular economy
            elif gdppc > 20000:
                base_circularity = 9.0
            elif gdppc > 8000:
                base_circularity = 6.0
            elif gdppc > 2000:
                base_circularity = 4.0
            else:
                base_circularity = 2.5     # low recycling infrastructure

            # Energy intensity adjustment: higher energy intensity = lower circularity
            ei_val = ei_ts[sorted(ei_ts.keys())[-1]] if ei_ts else None
            ei_adjustment = 0.0
            if ei_val is not None:
                # Energy intensity (MJ per constant 2017 PPP USD)
                # Global avg ~5.5, EU ~4.0
                if ei_val < 4.0:
                    ei_adjustment = 1.5   # efficient = more circular
                elif ei_val > 8.0:
                    ei_adjustment = -2.0

            # Industry share adjustment
            ind_val = industry_ts[sorted(industry_ts.keys())[-1]] if industry_ts else None
            ind_adjustment = 0.0
            if ind_val is not None:
                if ind_val > 35:
                    ind_adjustment = -1.5  # heavy industry = linear economy
                elif ind_val < 20:
                    ind_adjustment = 1.0   # service economy = lighter material footprint

            est_circularity = base_circularity + ei_adjustment + ind_adjustment
            est_circularity = float(np.clip(est_circularity, 1.0, 25.0))

            # Trend: CO2 intensity trend as proxy for decoupling
            decoupling_trend = None
            if co2_ts and gdp_ts and len(set(co2_ts.keys()) & set(gdp_ts.keys())) >= 5:
                common = sorted(set(co2_ts.keys()) & set(gdp_ts.keys()))
                co2_vals = np.array([co2_ts[y] for y in common])
                gdp_vals = np.array([gdp_ts[y] for y in common])
                co2_intensity = co2_vals / gdp_vals  # kt per USD
                t_arr = np.arange(len(co2_intensity), dtype=float)
                sl, _, r, _, _ = linregress(t_arr, co2_intensity)
                decoupling_trend = {
                    "slope": round(float(sl), 8),
                    "r_squared": round(float(r) ** 2, 3),
                    "direction": "decoupling" if sl < 0 else "coupling",
                }

            circularity_rate = {
                "estimated_circularity_pct": round(est_circularity, 2),
                "eu_benchmark_pct": self.EU_CIRCULARITY_BENCHMARK,
                "global_avg_pct": self.GLOBAL_AVG_CIRCULARITY,
                "gap_to_eu_benchmark": round(max(0.0, self.EU_CIRCULARITY_BENCHMARK - est_circularity), 2),
                "circularity_classification": (
                    "advanced" if est_circularity > 12
                    else "developing" if est_circularity > 6
                    else "nascent"
                ),
                "energy_intensity_adjustment": round(ei_adjustment, 2),
                "industry_share_adjustment": round(ind_adjustment, 2),
                "decoupling_trend": decoupling_trend,
            }

            # --- Waste-to-resource economics ---
            # Waste generation: ~0.74 kg/person/day (World Bank What a Waste 2.0)
            # Increases with income
            if gdppc > 30000:
                waste_kg_per_day = 1.60
            elif gdppc > 10000:
                waste_kg_per_day = 1.00
            elif gdppc > 3000:
                waste_kg_per_day = 0.65
            else:
                waste_kg_per_day = 0.40

            annual_waste_mt = waste_kg_per_day * pop * 365 / 1e9   # million tonnes

            # Recycling rate proxy (from circularity estimate)
            recycling_rate = est_circularity / 100.0 * 2   # rough conversion

            # Revenue from recovered materials
            avg_material_value_per_tonne = 120.0 * (gdppc / 10000) ** 0.3
            recovered_mt = annual_waste_mt * recycling_rate
            recycling_revenue = recovered_mt * 1e6 * avg_material_value_per_tonne

            # Cost of landfill disposal avoided
            landfill_cost_per_tonne = 30.0 * (gdppc / 10000) ** 0.5
            landfill_cost_avoided = recovered_mt * 1e6 * landfill_cost_per_tonne

            net_waste_economy = recycling_revenue + landfill_cost_avoided

            waste_economics = {
                "annual_waste_generation_mt": round(annual_waste_mt, 2),
                "waste_per_capita_kg_day": round(waste_kg_per_day, 2),
                "estimated_recycling_rate_pct": round(recycling_rate * 100, 1),
                "recovered_materials_mt": round(recovered_mt, 3),
                "recycling_revenue_musd": round(recycling_revenue / 1e6, 2),
                "landfill_cost_avoided_musd": round(landfill_cost_avoided / 1e6, 2),
                "net_waste_economy_value_musd": round(net_waste_economy / 1e6, 2),
                "value_pct_gdp": round(net_waste_economy / gdp_val * 100, 4),
            }

            # --- EPR assessment ---
            # Extended Producer Responsibility: producer responsible for EoL
            # Effectiveness depends on: legal framework, enforcement capacity, income
            if gdppc > 20000:
                epr_effectiveness = 0.70  # strong enforcement
                epr_coverage_pct = 60.0   # % of product categories covered
            elif gdppc > 8000:
                epr_effectiveness = 0.45
                epr_coverage_pct = 35.0
            elif gdppc > 2000:
                epr_effectiveness = 0.20
                epr_coverage_pct = 15.0
            else:
                epr_effectiveness = 0.05
                epr_coverage_pct = 5.0

            # EPR scheme economics: producer fees fund collection
            epr_fee_per_product_usd = 2.5 * (gdppc / 10000) ** 0.4
            products_in_market = gdp_val / 1000.0   # proxy: $1000 of GDP = 1 product unit
            epr_fund_potential = products_in_market * epr_fee_per_product_usd * (
                epr_coverage_pct / 100
            )

            epr_assessment = {
                "estimated_epr_effectiveness": round(epr_effectiveness, 2),
                "product_coverage_pct": round(epr_coverage_pct, 1),
                "epr_fund_potential_musd": round(epr_fund_potential / 1e6, 2),
                "enforcement_capacity": (
                    "strong" if gdppc > 20000
                    else "moderate" if gdppc > 8000
                    else "weak"
                ),
                "recommended_instruments": (
                    ["deposit_refund", "take_back_mandate", "eco_design"]
                    if gdppc > 10000
                    else ["fee_waiver", "informal_sector_integration", "public_collection"]
                ),
            }

            # --- Circular employment ---
            # ILO: circular economy creates 6 million net jobs globally
            # Repair/reuse/remanufacturing: 10-30x more labour than landfill
            circular_jobs_per_1000_linear = 15   # jobs in repair vs 1 in landfill
            repair_economy_share = est_circularity / 100.0
            repair_workers = pop * repair_economy_share * 0.02   # 2% of pop engaged

            # Labour income multiplier
            avg_wage_repair = gdppc * 0.8   # repair workers earn ~80% of GDPpc
            labour_income_circular = repair_workers * avg_wage_repair

            # Green jobs in waste management
            formal_waste_workers = recovered_mt * 1e6 / 1000  # 1 worker per 1000 tonnes
            waste_management_income = formal_waste_workers * avg_wage_repair * 0.6

            employment_impact = {
                "circular_economy_workers": round(repair_workers, 0),
                "formal_waste_management_workers": round(formal_waste_workers, 0),
                "total_circular_jobs": round(repair_workers + formal_waste_workers, 0),
                "circular_jobs_pct_workforce": round(
                    (repair_workers + formal_waste_workers) / (pop * 0.50) * 100, 2
                ),
                "labour_income_musd": round(
                    (labour_income_circular + waste_management_income) / 1e6, 2
                ),
                "jobs_vs_linear_multiplier": circular_jobs_per_1000_linear,
            }

        # --- Score ---
        score = 30.0

        if circularity_rate:
            cr = circularity_rate["estimated_circularity_pct"]
            if cr < 3:
                score += 35
            elif cr < 7:
                score += 20
            elif cr < 12:
                score += 10

        if waste_economics:
            value_pct = waste_economics["value_pct_gdp"]
            if value_pct < 0.1:
                score += 15  # very low value recovery = large gap
            elif value_pct < 0.5:
                score += 8

        if epr_assessment:
            if epr_assessment["enforcement_capacity"] == "weak":
                score += 15
            elif epr_assessment["enforcement_capacity"] == "moderate":
                score += 7

        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": target,
                "circularity_rate": circularity_rate,
                "waste_to_resource_economics": waste_economics,
                "epr_assessment": epr_assessment,
                "circular_employment": employment_impact,
            },
        }
