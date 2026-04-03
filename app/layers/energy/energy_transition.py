"""Energy transition: technology diffusion, investment needs, job impacts, just transition.

Methodology
-----------
**Technology diffusion (Bass model)** (Bass 1969):
Models adoption of new energy technologies (solar, wind, EVs, heat pumps)
as an S-curve driven by innovation (p) and imitation (q) coefficients:

    F(t) = [1 - exp(-(p+q)*t)] / [1 + (q/p)*exp(-(p+q)*t)]
    f(t) = F'(t) = [(p+q)^2 / q] * [exp(-(p+q)*t)] / [1 + (q/p)*exp(-(p+q)*t)]^2

where:
    F(t) = cumulative adoption fraction at time t
    f(t) = adoption rate (new adopters per period)
    p    = coefficient of innovation (external influence, ~0.01-0.05)
    q    = coefficient of imitation (internal influence, ~0.1-0.5)
    m    = market potential (total addressable market)

Peak adoption at t* = ln(q/p) / (p+q).

Fit to historical deployment data via nonlinear least squares (scipy).

**Investment needs estimation**:
Gap between current annual clean energy investment and the level needed for
net-zero by 2050 (IEA NZE scenario). Decomposed by technology:
    investment_gap = required_annual - current_annual
    cumulative_gap = sum over years to 2050

**Job creation/destruction** (IRENA methodology):
    direct_jobs = installed_capacity * jobs_per_MW (technology-specific)
    indirect_jobs = direct_jobs * multiplier
    lost_fossil_jobs = fossil_capacity_retired * fossil_jobs_per_MW

    Net employment = (direct + indirect) - lost_fossil_jobs

    Technology-specific employment factors (jobs/MW):
    Solar PV ~25, Onshore wind ~5, Offshore wind ~15, Nuclear ~0.5

**Just transition index** (composite):
    Weighted average of:
    - Worker displacement risk (fossil employment share)
    - Regional concentration (HHI of fossil activity)
    - Retraining capacity (education spending, vocational programs)
    - Social protection (unemployment benefits generosity)
    - Community diversification (economic complexity of fossil regions)

Score reflects transition risk: slow diffusion, large investment gaps,
job losses without retraining, and poor just transition raise the score.

Sources: IRENA, IEA, BloombergNEF, national energy statistics
"""

import numpy as np
from scipy import optimize

from app.layers.base import LayerBase


def _bass_cumulative(t: np.ndarray, p: float, q: float, m: float) -> np.ndarray:
    """Bass model cumulative adoption: F(t) * m."""
    exp_term = np.exp(-(p + q) * t)
    return m * (1 - exp_term) / (1 + (q / p) * exp_term)


def _fit_bass(times: np.ndarray, cumulative: np.ndarray, m_guess: float) -> dict | None:
    """Fit Bass model to cumulative adoption data via nonlinear least squares."""
    if len(times) < 5 or m_guess <= 0:
        return None

    # Normalize time to start at 0
    t = times - times[0]

    try:
        popt, pcov = optimize.curve_fit(
            _bass_cumulative, t, cumulative,
            p0=[0.03, 0.3, m_guess],
            bounds=([1e-5, 1e-3, cumulative[-1] * 0.8],
                    [0.5, 2.0, m_guess * 3]),
            maxfev=5000,
        )
        p_est, q_est, m_est = popt
        residuals = cumulative - _bass_cumulative(t, *popt)
        ss_res = float(np.sum(residuals ** 2))
        ss_tot = float(np.sum((cumulative - np.mean(cumulative)) ** 2))
        r_sq = 1 - ss_res / ss_tot if ss_tot > 0 else 0

        # Peak adoption time
        peak_t = np.log(q_est / p_est) / (p_est + q_est) if q_est > p_est else 0
        peak_year = float(times[0] + peak_t)

        # Forecast: years to 50%, 80%, 95% of market potential
        milestones = {}
        for pct in [0.5, 0.8, 0.95]:
            target = m_est * pct
            if cumulative[-1] < target:
                # Solve F(t) = target
                try:
                    sol = optimize.brentq(
                        lambda tt: _bass_cumulative(np.array([tt]), p_est, q_est, m_est)[0] - target,
                        0, 200
                    )
                    milestones[f"year_{int(pct*100)}pct"] = round(float(times[0] + sol), 1)
                except ValueError:
                    pass

        return {
            "p_innovation": round(float(p_est), 4),
            "q_imitation": round(float(q_est), 4),
            "market_potential": round(float(m_est), 1),
            "peak_adoption_year": round(peak_year, 1),
            "r_squared": round(r_sq, 4),
            "current_penetration_pct": round(float(cumulative[-1] / m_est * 100), 1),
            "milestones": milestones,
        }
    except (RuntimeError, ValueError):
        return None


class EnergyTransition(LayerBase):
    layer_id = "l16"
    name = "Energy Transition"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")

        # Employment factors (jobs per MW installed capacity)
        jobs_per_mw = {
            "solar": 25.0, "wind_onshore": 5.0, "wind_offshore": 15.0,
            "nuclear": 0.5, "coal": 3.0, "gas": 1.5,
        }

        series_map = {
            "solar_capacity": f"SOLAR_CAPACITY_{country}",
            "wind_capacity": f"WIND_CAPACITY_{country}",
            "ev_stock": f"EV_STOCK_{country}",
            "heat_pump_stock": f"HEAT_PUMP_STOCK_{country}",
            "solar_potential": f"SOLAR_POTENTIAL_{country}",
            "wind_potential": f"WIND_POTENTIAL_{country}",
            "ev_potential": f"EV_POTENTIAL_{country}",
            "hp_potential": f"HEAT_PUMP_POTENTIAL_{country}",
            "clean_investment": f"CLEAN_ENERGY_INVESTMENT_{country}",
            "required_investment": f"NZE_REQUIRED_INVESTMENT_{country}",
            "fossil_capacity_coal": f"CAPACITY_COAL_{country}",
            "fossil_capacity_gas": f"CAPACITY_GAS_{country}",
            "retired_capacity": f"FOSSIL_RETIRED_CAPACITY_{country}",
            "fossil_employment": f"FOSSIL_EMPLOYMENT_{country}",
            "total_employment": f"TOTAL_EMPLOYMENT_{country}",
            "education_spending": f"EDUCATION_SPENDING_PCT_GDP_{country}",
            "unemployment_benefits": f"UNEMPLOYMENT_BENEFIT_REPLACEMENT_{country}",
            "fossil_region_hhi": f"FOSSIL_REGION_HHI_{country}",
            "vocational_enrollment": f"VOCATIONAL_ENROLLMENT_{country}",
        }
        data = {}
        for label, code in series_map.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
                (code,),
            )
            if rows:
                data[label] = {r[0]: float(r[1]) for r in rows}

        results = {"country": country}

        # --- Technology diffusion (Bass model) ---
        tech_configs = [
            ("solar", "solar_capacity", "solar_potential"),
            ("wind", "wind_capacity", "wind_potential"),
            ("ev", "ev_stock", "ev_potential"),
            ("heat_pump", "heat_pump_stock", "hp_potential"),
        ]
        diffusion_results = {}
        for tech_name, data_key, potential_key in tech_configs:
            if data_key not in data:
                continue

            dates_sorted = sorted(data[data_key].keys())
            vals = np.array([data[data_key][d] for d in dates_sorted])
            times = np.array([float(d[:4]) for d in dates_sorted])

            # Market potential
            m_guess = float(list(data[potential_key].values())[-1]) if potential_key in data else vals[-1] * 5

            bass_fit = _fit_bass(times, vals, m_guess)
            if bass_fit:
                diffusion_results[tech_name] = bass_fit

        if diffusion_results:
            results["technology_diffusion"] = diffusion_results

        # --- Investment needs ---
        if "clean_investment" in data and "required_investment" in data:
            common_inv = sorted(set(data["clean_investment"]) & set(data["required_investment"]))
            if common_inv:
                latest = common_inv[-1]
                current = data["clean_investment"][latest]
                required = data["required_investment"][latest]
                gap = required - current
                gap_pct = (gap / required * 100) if required > 0 else 0

                # Trend
                if len(common_inv) >= 3:
                    inv_vals = np.array([data["clean_investment"][d] for d in common_inv])
                    t_arr = np.arange(len(inv_vals), dtype=float)
                    growth = float(np.polyfit(t_arr, inv_vals, 1)[0])
                    years_to_close = gap / growth if growth > 0 else float("inf")
                else:
                    growth = 0
                    years_to_close = float("inf")

                results["investment_gap"] = {
                    "current_annual_bn": round(current, 1),
                    "required_annual_bn": round(required, 1),
                    "gap_bn": round(float(gap), 1),
                    "gap_pct": round(float(gap_pct), 1),
                    "annual_growth_bn": round(float(growth), 1),
                    "years_to_close_gap": round(float(years_to_close), 1)
                    if years_to_close < 100 else None,
                    "on_track": gap <= 0,
                    "date": latest,
                }

        # --- Job creation/destruction ---
        # Clean energy jobs created
        clean_jobs = 0
        job_details = {}
        for tech_name, data_key, _ in tech_configs[:2]:  # solar and wind
            if data_key in data:
                vals = list(data[data_key].values())
                capacity = float(vals[-1]) if vals else 0
                jpm = jobs_per_mw.get(tech_name, jobs_per_mw.get(f"{tech_name}_onshore", 5))
                direct = capacity * jpm
                indirect = direct * 0.5  # indirect multiplier ~0.5
                clean_jobs += direct + indirect
                job_details[tech_name] = {
                    "capacity_mw": round(capacity, 0),
                    "direct_jobs": round(float(direct), 0),
                    "indirect_jobs": round(float(indirect), 0),
                }

        # Fossil jobs at risk
        fossil_jobs = 0
        for fuel in ["coal", "gas"]:
            cap_key = f"fossil_capacity_{fuel}"
            if cap_key in data:
                vals = list(data[cap_key].values())
                cap = float(vals[-1]) if vals else 0
                jpm = jobs_per_mw.get(fuel, 2.0)
                fossil_jobs += cap * jpm

        if "fossil_employment" in data:
            fossil_emp_vals = list(data["fossil_employment"].values())
            fossil_jobs = float(fossil_emp_vals[-1]) if fossil_emp_vals else fossil_jobs

        net_jobs = clean_jobs - fossil_jobs

        if job_details or fossil_jobs > 0:
            results["employment_impact"] = {
                "clean_energy_jobs": round(float(clean_jobs), 0),
                "by_technology": job_details,
                "fossil_jobs_at_risk": round(float(fossil_jobs), 0),
                "net_employment_change": round(float(net_jobs), 0),
                "net_positive": net_jobs > 0,
            }

        # --- Just transition index ---
        jt_components = {}
        jt_scores = []

        # Worker displacement risk (fossil employment share)
        if "fossil_employment" in data and "total_employment" in data:
            common_emp = sorted(set(data["fossil_employment"]) & set(data["total_employment"]))
            if common_emp:
                latest = common_emp[-1]
                fossil_share = data["fossil_employment"][latest] / data["total_employment"][latest]
                displacement_score = min(float(fossil_share) * 500, 100)  # 20% share = 100
                jt_components["worker_displacement_risk"] = round(displacement_score, 1)
                jt_scores.append(displacement_score)

        # Regional concentration (HHI of fossil activity)
        if "fossil_region_hhi" in data:
            hhi_vals = list(data["fossil_region_hhi"].values())
            hhi = float(hhi_vals[-1]) if hhi_vals else 0
            # HHI 0-10000, normalize: >2500 = concentrated
            concentration_score = min(hhi / 100, 100)
            jt_components["regional_concentration"] = round(concentration_score, 1)
            jt_scores.append(concentration_score)

        # Retraining capacity
        if "education_spending" in data or "vocational_enrollment" in data:
            retrain_score = 50  # default moderate
            if "education_spending" in data:
                edu_vals = list(data["education_spending"].values())
                edu = float(edu_vals[-1]) if edu_vals else 4
                retrain_score = max(100 - edu * 15, 0)  # higher spending = lower risk
            if "vocational_enrollment" in data:
                voc_vals = list(data["vocational_enrollment"].values())
                voc = float(voc_vals[-1]) if voc_vals else 0
                retrain_score = (retrain_score + max(100 - voc * 2, 0)) / 2
            jt_components["retraining_deficit"] = round(float(retrain_score), 1)
            jt_scores.append(retrain_score)

        # Social protection
        if "unemployment_benefits" in data:
            ben_vals = list(data["unemployment_benefits"].values())
            replacement = float(ben_vals[-1]) if ben_vals else 0
            protection_score = max(100 - replacement * 1.5, 0)  # higher replacement = lower risk
            jt_components["social_protection_deficit"] = round(float(protection_score), 1)
            jt_scores.append(protection_score)

        if jt_scores:
            jt_index = float(np.mean(jt_scores))
            results["just_transition"] = {
                "index": round(jt_index, 1),
                "components": jt_components,
                "just_transition_adequate": jt_index < 40,
            }

        # --- Score ---
        score = 15.0

        # Technology diffusion (slow = higher score)
        diff_info = results.get("technology_diffusion", {})
        if diff_info:
            avg_penetration = float(np.mean([
                d.get("current_penetration_pct", 50) for d in diff_info.values()
            ]))
            score += max((100 - avg_penetration) * 0.15, 0)

        # Investment gap
        inv_info = results.get("investment_gap", {})
        if inv_info:
            gap_pct = inv_info.get("gap_pct", 0) or 0
            score += min(gap_pct * 0.3, 20)

        # Net employment
        emp_info = results.get("employment_impact", {})
        if emp_info:
            if not emp_info.get("net_positive"):
                score += 10

        # Just transition
        jt_info = results.get("just_transition", {})
        if jt_info:
            jt_idx = jt_info.get("index", 50)
            score += min(jt_idx * 0.2, 15)

        score = float(np.clip(score, 0, 100))

        return {"score": round(score, 1), "results": results}
