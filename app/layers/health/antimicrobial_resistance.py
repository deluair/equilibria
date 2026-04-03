"""Antimicrobial resistance (AMR): economic cost projections, R&D pipeline, One Health.

Projects the macroeconomic cost of AMR using the O'Neill Commission methodology.
Tracks antibiotic consumption trends using DDD (defined daily doses) analysis.
Assesses pharmaceutical R&D pipeline adequacy against WHO priority pathogens.
Scores countries on One Health framework integration across human-animal-
environment interfaces.

Key references:
    O'Neill, J. (2016). Tackling Drug-Resistant Infections Globally: Final
        Report and Recommendations. Review on Antimicrobial Resistance.
    Laxminarayan, R. et al. (2013). Antibiotic resistance: the need for global
        solutions. Lancet, 382(9912), 1057-1098.
    WHO (2019). Antibacterial agents in clinical and preclinical development:
        an overview and analysis. Geneva.
    De Kraker, M.E.A., Stewardson, A.J. & Harbath, S. (2016). Will 10 million
        people die a year due to antimicrobial resistance by 2050? PLOS Medicine.
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class AntimicrobialResistance(LayerBase):
    layer_id = "l8"
    name = "Antimicrobial Resistance"
    weight = 0.20

    # O'Neill (2016) baseline AMR attribution deaths globally
    BASELINE_AMR_DEATHS_GLOBAL = 700_000   # per year currently
    PROJECTED_AMR_DEATHS_2050 = 10_000_000

    # Antibiotic consumption DDD thresholds (per 1,000 inhabitants per day)
    # High consumption associated with resistance emergence
    HIGH_CONSUMPTION_DDD = 30.0
    MODERATE_CONSUMPTION_DDD = 15.0

    # WHO priority pathogen groups (2017)
    CRITICAL_PATHOGENS = ["Acinetobacter", "Pseudomonas", "Enterobacteriaceae"]
    HIGH_PRIORITY_PATHOGENS = ["Enterococcus", "Staphylococcus", "Helicobacter", "Campylobacter"]

    async def compute(self, db, **kwargs) -> dict:
        """Compute AMR economic burden, consumption trends, and R&D pipeline.

        Fetches GDP, population, health expenditure, and antibiotic use proxies.
        Projects AMR mortality and GDP cost using O'Neill methodology. Estimates
        R&D pipeline adequacy. Scores One Health framework integration.

        Returns dict with score, economic_cost, consumption_analysis, rd_pipeline,
        and one_health_score.
        """
        country_iso3 = kwargs.get("country_iso3")

        # GDP (total and per capita)
        gdp_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.MKTP.KD'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        gdppc_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        pop_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.POP.TOTL'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Health expenditure (% GDP) - proxy for surveillance/stewardship investment
        health_exp_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.XPD.CHEX.GD.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Hospital beds per 1,000 (proxy for nosocomial infection risk)
        beds_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.MED.BEDS.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not gdp_rows and not gdppc_rows:
            return {"score": 50, "results": {"error": "no GDP data"}}

        def _index(rows):
            out: dict[str, dict[str, float]] = {}
            for r in rows:
                out.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]
            return out

        gdp_data = _index(gdp_rows) if gdp_rows else {}
        gdppc_data = _index(gdppc_rows) if gdppc_rows else {}
        pop_data = _index(pop_rows) if pop_rows else {}
        health_exp_data = _index(health_exp_rows) if health_exp_rows else {}
        beds_data = _index(beds_rows) if beds_rows else {}

        economic_cost = None
        consumption_analysis = None
        rd_pipeline = None
        one_health_score = None

        target = country_iso3

        gdp_ts = gdp_data.get(target, {}) if target else {}
        gdppc_ts = gdppc_data.get(target, {}) if target else {}
        pop_ts = pop_data.get(target, {}) if target else {}

        if gdppc_ts and pop_ts:
            latest_yr = sorted(
                set(gdppc_ts.keys()) & set(pop_ts.keys())
            )[-1]
            gdppc = gdppc_ts[latest_yr]
            pop = pop_ts[latest_yr]

            # --- AMR economic cost projection (O'Neill methodology) ---
            # Global AMR cost: ~$100 trillion cumulative by 2050 (O'Neill 2016)
            # Country share: proportional to GDP share
            global_gdp_estimate = 100e12  # USD
            country_gdp = (
                gdp_ts.get(latest_yr, gdppc * pop)
                if gdp_ts else gdppc * pop
            )
            gdp_share = country_gdp / global_gdp_estimate

            # Current annual AMR cost: ~$55B globally (estimated)
            global_annual_amr_cost = 55e9
            country_annual_amr_cost = global_annual_amr_cost * gdp_share

            # Mortality share: population-proportional + income penalty
            # LMICs bear disproportionate burden (higher prevalence, weaker surveillance)
            global_pop = 8e9
            pop_share = pop / global_pop
            income_burden_multiplier = max(0.5, min(3.0, 5000 / max(gdppc, 100)))
            country_amr_deaths = (
                self.BASELINE_AMR_DEATHS_GLOBAL * pop_share * income_burden_multiplier
            )

            # 2050 projection: GDP loss
            # O'Neill: low-income countries lose 3.8% GDP, high-income 0.4%
            if gdppc < 2000:
                gdp_loss_pct_2050 = 3.8
            elif gdppc < 10000:
                gdp_loss_pct_2050 = 1.5
            else:
                gdp_loss_pct_2050 = 0.4

            gdp_loss_2050 = country_gdp * gdp_loss_pct_2050 / 100

            # Extended treatment costs: resistant infections cost 3-5x more
            excess_treatment_cost_per_patient = gdppc * 0.15  # 15% GDPpc per case
            estimated_amr_patients = country_amr_deaths * 10  # 10x case-fatality ratio
            total_treatment_burden = estimated_amr_patients * excess_treatment_cost_per_patient

            economic_cost = {
                "year": latest_yr,
                "gdp_per_capita_usd": float(gdppc),
                "current_annual_amr_cost_musd": round(country_annual_amr_cost / 1e6, 2),
                "current_amr_attributed_deaths": round(country_amr_deaths, 0),
                "projected_gdp_loss_2050_pct": gdp_loss_pct_2050,
                "projected_gdp_loss_2050_musd": round(gdp_loss_2050 / 1e6, 2),
                "excess_treatment_cost_musd": round(total_treatment_burden / 1e6, 2),
                "amr_cost_pct_gdp": round(
                    (country_annual_amr_cost + total_treatment_burden) / country_gdp * 100, 4
                ),
            }

            # --- Antibiotic consumption trend analysis ---
            # Use health expenditure as a proxy for consumption capacity
            # High-income + high health spending -> higher antibiotic use
            # Note: direct ECDC/IQVIA DDD data not in standard WDI
            he_ts = health_exp_data.get(target, {})
            he_val = he_ts[sorted(he_ts.keys())[-1]] if he_ts else None

            # Proxy DDD estimate from income group
            if gdppc < 1000:
                est_ddd = 8.0    # low access but also low quality
            elif gdppc < 5000:
                est_ddd = 18.0   # increasing use with income rise
            elif gdppc < 20000:
                est_ddd = 28.0   # peak consumption (over-prescription risk)
            else:
                est_ddd = 22.0   # stewardship programs reduce use

            # Trend: higher health expenditure growth = faster consumption growth
            if he_ts and len(he_ts) >= 5:
                he_yrs = sorted(he_ts.keys())
                he_vals = np.array([he_ts[y] for y in he_yrs])
                t_arr = np.arange(len(he_vals), dtype=float)
                sl, _, _, _, _ = linregress(t_arr, he_vals)
                consumption_trend = "increasing" if sl > 0.1 else (
                    "decreasing" if sl < -0.1 else "stable"
                )
            else:
                sl = 0.0
                consumption_trend = "unknown"

            consumption_analysis = {
                "estimated_ddd_per_1000_per_day": round(est_ddd, 1),
                "who_threshold_high": self.HIGH_CONSUMPTION_DDD,
                "consumption_risk": (
                    "high" if est_ddd > self.HIGH_CONSUMPTION_DDD
                    else "moderate" if est_ddd > self.MODERATE_CONSUMPTION_DDD
                    else "low"
                ),
                "health_expenditure_trend": consumption_trend,
                "health_exp_pct_gdp": round(float(he_val), 2) if he_val else None,
                "stewardship_needed": bool(est_ddd > self.MODERATE_CONSUMPTION_DDD),
            }

        # --- R&D pipeline assessment ---
        # WHO (2019): 50 antibiotics in clinical development, 8 innovative
        # Translational probability: ~10-15% from Phase I to approval
        pipeline_antibiotics_total = 50
        innovative_antibiotics = 8
        who_priority_pathogen_coverage = {
            "critical": 0.40,      # 40% of critical pathogens have a candidate
            "high_priority": 0.55,
            "medium_priority": 0.70,
        }

        # Market failure: only ~$1B revenue/year vs ~$1-2B development cost
        development_cost_per_antibiotic = 1.2e9  # $1.2B avg
        expected_annual_revenue = 0.5e9           # $500M typical
        roi_antibiotic_rd = (expected_annual_revenue - development_cost_per_antibiotic * 0.10) / (
            development_cost_per_antibiotic
        )  # annualized net

        # GARDP/CARB-X funding: ~$200M/year vs $1B+ needed
        global_rd_funding_musd = 200
        rd_funding_gap_musd = 1000 - global_rd_funding_musd

        rd_pipeline = {
            "antibiotics_in_clinical_development": pipeline_antibiotics_total,
            "innovative_candidates": innovative_antibiotics,
            "innovation_rate_pct": round(innovative_antibiotics / pipeline_antibiotics_total * 100, 1),
            "pathogen_coverage": who_priority_pathogen_coverage,
            "market_roi_ratio": round(roi_antibiotic_rd, 3),
            "market_failure": bool(roi_antibiotic_rd < 0),
            "global_rd_funding_musd": global_rd_funding_musd,
            "rd_funding_gap_musd": rd_funding_gap_musd,
            "push_incentive_needed": True,   # delinked funding mechanism required
            "pull_incentive_needed": True,   # LEMISC / market entry reward
        }

        # --- One Health framework scoring ---
        # Integrates human, animal, environmental antibiotic stewardship
        # Proxy indicators: governance, health spending, agricultural intensity

        beds_ts = beds_data.get(target, {}) if target else {}
        beds_val = beds_ts[sorted(beds_ts.keys())[-1]] if beds_ts else None

        gdppc_val = None
        if gdppc_ts:
            gdppc_val = gdppc_ts[sorted(gdppc_ts.keys())[-1]]

        # One Health score (0=weak, 100=strong integration)
        oh_score = 50.0  # baseline

        # Human health: physician density proxy via health spending
        he_global = {iso: health_exp_data[iso][sorted(health_exp_data[iso].keys())[-1]]
                     for iso in health_exp_data if health_exp_data[iso]}
        if target and target in he_global:
            he_pct = he_global[target]
            oh_score += min(20.0, he_pct * 2)  # up to 20 points for health spending
            if he_pct < 3.0:
                oh_score -= 15.0  # penalize very low health spending

        # Hospital infrastructure: beds as proxy for infection control
        if beds_val is not None:
            if beds_val > 4:
                oh_score += 10.0
            elif beds_val < 1.5:
                oh_score -= 10.0

        # Income proxy for regulatory capacity
        if gdppc_val is not None:
            if gdppc_val > 20000:
                oh_score += 15.0  # stronger regulatory system
            elif gdppc_val < 2000:
                oh_score -= 10.0  # weaker regulation, informal antibiotic sales

        oh_score = float(np.clip(oh_score, 0, 100))

        one_health_score = {
            "composite_score": round(oh_score, 1),
            "interpretation": (
                "strong" if oh_score > 70
                else "moderate" if oh_score > 40
                else "weak"
            ),
            "hospital_beds_per_1000": float(beds_val) if beds_val is not None else None,
            "surveillance_capacity": (
                "adequate" if gdppc_val and gdppc_val > 10000
                else "limited" if gdppc_val and gdppc_val > 3000
                else "inadequate"
            ),
            "components": {
                "human_health_stewardship": bool(
                    he_global.get(target, 0) >= 4.0 if target else False
                ),
                "animal_health_integration": bool(gdppc_val and gdppc_val > 5000),
                "environmental_monitoring": bool(gdppc_val and gdppc_val > 10000),
            },
        }

        # --- Score ---
        # Higher score = higher AMR risk and burden
        score = 30.0

        if economic_cost:
            if economic_cost["projected_gdp_loss_2050_pct"] >= 3.0:
                score += 30
            elif economic_cost["projected_gdp_loss_2050_pct"] >= 1.5:
                score += 20
            else:
                score += 10

        if consumption_analysis:
            if consumption_analysis["consumption_risk"] == "high":
                score += 20
            elif consumption_analysis["consumption_risk"] == "moderate":
                score += 10

        if one_health_score and one_health_score["interpretation"] == "weak":
            score += 15
        elif one_health_score and one_health_score["interpretation"] == "moderate":
            score += 7

        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": target,
                "economic_cost": economic_cost,
                "antibiotic_consumption": consumption_analysis,
                "rd_pipeline": rd_pipeline,
                "one_health_score": one_health_score,
            },
        }
