"""Telemedicine economics: adoption rates, cost savings, rural access, care quality.

Models telehealth adoption trajectories using a diffusion-of-innovation framework.
Quantifies cost savings versus in-person visits via microeconomic substitution
analysis. Measures rural access improvement through reduction in travel burden.
Evaluates quality of care equivalence using outcome proxy benchmarks.

Key references:
    Bashshur, R.L. et al. (2014). The empirical evidence for telemedicine
        interventions in mental disorders. Telemedicine and e-Health, 22(2).
    Dorsey, E.R. & Topol, E.J. (2016). State of telehealth.
        NEJM, 375(2), 154-161.
    Totten, A.M. et al. (2019). Telehealth: Mapping the Evidence for Patient
        Outcomes from Systematic Reviews. AHRQ Technical Brief No. 34.
    Buvik, A. et al. (2018). Quality of care for remote orthopaedic consultations
        using telemedicine: a randomised controlled trial. BMC Health Services
        Research, 18, 67.
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class Telemedicine(LayerBase):
    layer_id = "l8"
    name = "Telemedicine"
    weight = 0.20

    # Cost ratio: telehealth visit / in-person visit
    TELEHEALTH_COST_RATIO = 0.50   # ~50% of in-person cost (McKinsey 2020)

    # Adoption diffusion parameters (Bass model calibration)
    INNOVATION_COEFF = 0.01    # p: external influence (advertising)
    IMITATION_COEFF = 0.38     # q: word of mouth

    # Rural accessibility improvement (hours saved per avoided trip)
    AVG_RURAL_TRAVEL_TIME_HOURS = 2.5
    TIME_VALUE_FRACTION_GDPpc = 0.45   # fraction of daily GDPpc for time value

    async def compute(self, db, **kwargs) -> dict:
        """Compute telemedicine adoption economics and access impact.

        Fetches internet access, GDP, population, health expenditure, and rural
        population data. Models adoption curve. Estimates cost savings and rural
        access improvement. Benchmarks quality of care.

        Returns dict with score, adoption_trajectory, cost_savings, rural_access,
        and care_quality_benchmark.
        """
        country_iso3 = kwargs.get("country_iso3")

        # Internet access (% population) - key enabler
        internet_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'IT.NET.USER.ZS'
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

        # Rural population (% of total)
        rural_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.RUR.TOTL.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Health expenditure (% GDP)
        health_exp_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.XPD.CHEX.GD.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Physicians per 1,000 (access gap = telemedicine opportunity)
        physician_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.MED.PHYS.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not internet_rows and not gdppc_rows:
            return {"score": 50, "results": {"error": "insufficient data for telemedicine analysis"}}

        def _index(rows):
            out: dict[str, dict[str, float]] = {}
            for r in rows:
                out.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]
            return out

        internet_data = _index(internet_rows) if internet_rows else {}
        gdppc_data = _index(gdppc_rows) if gdppc_rows else {}
        pop_data = _index(pop_rows) if pop_rows else {}
        rural_data = _index(rural_rows) if rural_rows else {}
        health_exp_data = _index(health_exp_rows) if health_exp_rows else {}
        physician_data = _index(physician_rows) if physician_rows else {}

        adoption_trajectory = None
        cost_savings = None
        rural_access = None
        care_quality_benchmark = None

        target = country_iso3
        internet_ts = internet_data.get(target, {}) if target else {}
        gdppc_ts = gdppc_data.get(target, {}) if target else {}
        pop_ts = pop_data.get(target, {}) if target else {}
        rural_ts = rural_data.get(target, {}) if target else {}
        he_ts = health_exp_data.get(target, {}) if target else {}
        phys_ts = physician_data.get(target, {}) if target else {}

        if gdppc_ts and pop_ts:
            latest_yr = sorted(set(gdppc_ts.keys()) & set(pop_ts.keys()))[-1]
            gdppc = gdppc_ts[latest_yr]
            pop = pop_ts[latest_yr]

            # Current internet penetration
            internet_pct = None
            if internet_ts:
                latest_internet_yr = sorted(internet_ts.keys())[-1]
                internet_pct = internet_ts[latest_internet_yr] / 100.0

            # --- Adoption trajectory (Bass diffusion model) ---
            # N(t): cumulative adopters, M = market potential = internet-enabled population
            M = pop * (internet_pct or 0.50)   # market potential
            p_coeff = self.INNOVATION_COEFF
            q_coeff = self.IMITATION_COEFF

            # Initial adoption: estimate from income (richer countries adopt faster)
            if gdppc > 30000:
                initial_adoption_pct = 0.25  # post-COVID mature market
            elif gdppc > 10000:
                initial_adoption_pct = 0.10
            elif gdppc > 3000:
                initial_adoption_pct = 0.04
            else:
                initial_adoption_pct = 0.01

            N0 = M * initial_adoption_pct

            # Simulate Bass model for 10 years
            horizon = 10
            N = np.zeros(horizon + 1)
            N[0] = N0
            for t in range(1, horizon + 1):
                dN = (p_coeff + q_coeff * N[t - 1] / M) * (M - N[t - 1])
                N[t] = min(M, N[t - 1] + dN)

            adoption_pct_series = (N / pop * 100).tolist()
            peak_adoption_pct = float(N[-1] / pop * 100)

            # Internet trend
            internet_trend = None
            if internet_ts and len(internet_ts) >= 5:
                yrs = sorted(internet_ts.keys())
                vals = np.array([internet_ts[y] for y in yrs])
                t_arr = np.arange(len(vals), dtype=float)
                sl, inter, r, _, _ = linregress(t_arr, vals)
                internet_trend = {
                    "slope_pct_per_year": round(float(sl), 2),
                    "latest_penetration_pct": round(float(vals[-1]), 1),
                    "r_squared": round(float(r) ** 2, 3),
                }

            adoption_trajectory = {
                "internet_penetration_pct": (
                    round(float(internet_pct) * 100, 1) if internet_pct else None
                ),
                "current_telehealth_adoption_pct": round(initial_adoption_pct * 100, 1),
                "projected_10yr_adoption_pct": round(peak_adoption_pct, 1),
                "bass_model_params": {"p_innovation": p_coeff, "q_imitation": q_coeff},
                "adoption_trajectory_pct": [round(x, 2) for x in adoption_pct_series],
                "internet_trend": internet_trend,
                "adopter_population_10yr": round(float(N[-1]), 0),
            }

            # --- Cost savings analysis ---
            he_pct = he_ts[sorted(he_ts.keys())[-1]] if he_ts else None
            health_spend_total = gdppc * pop * (he_pct / 100) if he_pct else None

            # Average consultation cost (in-person)
            if gdppc > 30000:
                consult_cost_inperson = 200.0
            elif gdppc > 10000:
                consult_cost_inperson = 80.0
            elif gdppc > 3000:
                consult_cost_inperson = 25.0
            else:
                consult_cost_inperson = 8.0

            consult_cost_telehealth = consult_cost_inperson * self.TELEHEALTH_COST_RATIO

            # Consultations per adopter per year (assume ~3 consultable visits)
            telehealth_consults_per_year = 2.5
            total_telehealth_consults = float(N[-1]) * telehealth_consults_per_year

            direct_savings = (
                total_telehealth_consults
                * (consult_cost_inperson - consult_cost_telehealth)
            )
            savings_pct_health = (
                direct_savings / health_spend_total * 100
                if health_spend_total else None
            )

            # Patient time savings (avoided travel)
            daily_gdppc = gdppc / 365
            time_value_per_hour = daily_gdppc * self.TIME_VALUE_FRACTION_GDPpc / 8
            avg_travel_savings_hours = 1.5   # urban avg travel avoided
            patient_time_savings = (
                total_telehealth_consults * avg_travel_savings_hours * time_value_per_hour
            )

            cost_savings = {
                "inperson_consult_cost_usd": round(consult_cost_inperson, 2),
                "telehealth_consult_cost_usd": round(consult_cost_telehealth, 2),
                "annual_telehealth_consults_10yr": round(total_telehealth_consults, 0),
                "direct_cost_savings_musd": round(direct_savings / 1e6, 2),
                "patient_time_savings_musd": round(patient_time_savings / 1e6, 2),
                "total_economic_benefit_musd": round(
                    (direct_savings + patient_time_savings) / 1e6, 2
                ),
                "savings_pct_health_expenditure": (
                    round(float(savings_pct_health), 3) if savings_pct_health else None
                ),
            }

            # --- Rural access improvement ---
            rural_pct = None
            if rural_ts:
                rural_pct = rural_ts[sorted(rural_ts.keys())[-1]] / 100.0

            rural_pop = pop * (rural_pct or 0.35)
            rural_internet_pct = (internet_pct or 0.50) * 0.60  # rural lag
            rural_telehealth_adopters = rural_pop * rural_internet_pct * initial_adoption_pct

            phys_per_1000 = None
            if phys_ts:
                phys_per_1000 = phys_ts[sorted(phys_ts.keys())[-1]]

            # Effective physician shortage in rural areas (typically 4x worse)
            rural_phys_per_1000 = (phys_per_1000 or 1.0) / 4.0
            rural_physician_gap = max(0.0, 2.3 - rural_phys_per_1000)

            # Consultations unlocked by telehealth for rural population
            rural_consults_unlocked = rural_telehealth_adopters * telehealth_consults_per_year
            travel_time_saved_hours = (
                rural_consults_unlocked * self.AVG_RURAL_TRAVEL_TIME_HOURS
            )
            travel_cost_saved = (
                travel_time_saved_hours * time_value_per_hour
                + rural_consults_unlocked * consult_cost_inperson * 0.10  # transport cost
            )

            rural_access = {
                "rural_population_pct": round((rural_pct or 0.35) * 100, 1),
                "rural_internet_penetration_est_pct": round(rural_internet_pct * 100, 1),
                "rural_telehealth_adopters": round(rural_telehealth_adopters, 0),
                "rural_physician_per_1000_est": round(rural_phys_per_1000, 3),
                "rural_physician_gap_to_who": round(rural_physician_gap, 3),
                "annual_rural_consults_unlocked": round(rural_consults_unlocked, 0),
                "travel_time_saved_million_hours": round(
                    travel_time_saved_hours / 1e6, 2
                ),
                "travel_cost_saved_musd": round(travel_cost_saved / 1e6, 2),
                "access_improvement_multiplier": round(
                    1.0 + rural_consults_unlocked / max(1, rural_pop * rural_phys_per_1000 * 4),
                    2,
                ),
            }

        # --- Quality of care equivalence benchmark ---
        # Dorsey & Topol (2016): telehealth non-inferior for chronic disease,
        # mental health, dermatology. Inferior for emergency, physical examination.
        quality_domains = {
            "chronic_disease_management": {
                "evidence_level": "strong",
                "non_inferiority": True,
                "best_use_cases": ["diabetes", "hypertension", "asthma"],
            },
            "mental_health": {
                "evidence_level": "strong",
                "non_inferiority": True,
                "best_use_cases": ["depression", "anxiety", "ptsd"],
            },
            "dermatology": {
                "evidence_level": "moderate",
                "non_inferiority": True,
                "best_use_cases": ["rash", "wound_follow_up", "skin_cancer_screening"],
            },
            "acute_care": {
                "evidence_level": "moderate",
                "non_inferiority": False,
                "limitations": "physical examination required",
            },
            "radiology": {
                "evidence_level": "strong",
                "non_inferiority": True,
                "note": "image transmission fully equivalent",
            },
        }

        # Country readiness for quality telehealth
        readiness_score = 40.0
        if gdppc_ts:
            gdppc_val = gdppc_ts[sorted(gdppc_ts.keys())[-1]]
            if gdppc_val > 20000:
                readiness_score += 30.0
            elif gdppc_val > 5000:
                readiness_score += 15.0

        if internet_ts:
            latest_ip = internet_ts[sorted(internet_ts.keys())[-1]]
            readiness_score += min(20.0, latest_ip * 0.20)

        readiness_score = float(np.clip(readiness_score, 0, 100))

        care_quality_benchmark = {
            "readiness_score": round(readiness_score, 1),
            "quality_domains": quality_domains,
            "overall_non_inferiority": True,
            "evidence_base": "Totten et al. (2019) AHRQ systematic review",
            "quality_assurance_requirements": [
                "digital_literacy_training",
                "data_privacy_regulation",
                "clinical_protocol_adaptation",
                "broadband_infrastructure",
            ],
        }

        # --- Score (higher = higher unmet telehealth potential / burden) ---
        score = 30.0

        if adoption_trajectory:
            current_pct = adoption_trajectory["current_telehealth_adoption_pct"]
            if current_pct < 2:
                score += 25   # very low adoption = high unmet potential
            elif current_pct < 8:
                score += 15
            elif current_pct < 20:
                score += 8

        if rural_access:
            rural_gap = rural_access.get("rural_physician_gap_to_who", 0)
            if rural_gap > 2:
                score += 25
            elif rural_gap > 1:
                score += 15
            elif rural_gap > 0:
                score += 8

        if cost_savings:
            savings_pct = cost_savings.get("savings_pct_health_expenditure")
            if savings_pct and savings_pct > 5:
                score += 10   # high savings potential = large efficiency gain unrealized

        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": target,
                "adoption_trajectory": adoption_trajectory,
                "cost_savings": cost_savings,
                "rural_access_improvement": rural_access,
                "care_quality_benchmark": care_quality_benchmark,
            },
        }
