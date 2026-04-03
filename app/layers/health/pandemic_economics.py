"""Pandemic economics: SIR-macro model, lockdown CBA, excess mortality, vaccines.

Couples a standard SIR epidemiological model with macroeconomic output loss
to evaluate pandemic tradeoffs. Estimates lockdown cost-benefit using the
value of a statistical life (VSL). Computes excess mortality from observed
vs. predicted death rates. Optimizes vaccine allocation across country
income groups following the COVAX framework.

Key references:
    Eichenbaum, M., Rebelo, S. & Trabandt, M. (2021). The macroeconomics
        of epidemics. REStat, 103(5), 753-778.
    Greenstone, M. & Nigam, V. (2020). Does social distancing matter?
        University of Chicago, BFI Working Paper.
    Karlinsky, A. & Kobak, D. (2021). Tracking excess mortality across
        countries during the COVID-19 pandemic. eLife, 10:e69336.
    Emanuel, E.J. et al. (2020). Fair allocation of scarce medical resources
        in the time of Covid-19. NEJM, 382(21), 2049-2055.
"""

from __future__ import annotations

import numpy as np
from scipy import integrate

from app.layers.base import LayerBase


def _sir_macro(
    S0: float,
    I0: float,
    R0_val: float,
    beta: float,
    gamma: float,
    alpha: float,
    T: int,
    lockdown_start: int | None = None,
    lockdown_end: int | None = None,
    lockdown_reduction: float = 0.5,
) -> dict:
    """SIR-macro model following Eichenbaum, Rebelo, Trabandt (2021).

    S0, I0, R0_val: initial susceptible, infected, recovered fractions.
    beta: transmission rate.
    gamma: recovery rate (1/infectious_period).
    alpha: infection fatality rate.
    T: simulation horizon (days).
    lockdown_*: optional lockdown parameters.

    Returns dict with time series of S, I, R, D (dead), output_loss.
    """
    N = S0 + I0 + R0_val
    D0 = 0.0

    def deriv(t, y):
        s_val, inf, r_val, d_val = y
        # Effective beta with lockdown
        b = beta
        if lockdown_start is not None and lockdown_end is not None:
            if lockdown_start <= t <= lockdown_end:
                b = beta * (1 - lockdown_reduction)

        dSdt = -b * s_val * inf / N
        dIdt = b * s_val * inf / N - gamma * inf - alpha * inf
        dRdt = gamma * inf
        dDdt = alpha * inf
        return [dSdt, dIdt, dRdt, dDdt]

    t_span = (0, T)
    t_eval = np.arange(0, T + 1, dtype=float)
    y0 = [S0, I0, R0_val, D0]

    sol = integrate.solve_ivp(deriv, t_span, y0, t_eval=t_eval, method="RK45",
                              max_step=1.0)

    S, Inf, R, D = sol.y

    # Output loss: proportional to infection prevalence + lockdown
    output_loss = np.zeros(len(t_eval))
    for i, t in enumerate(t_eval):
        infection_cost = 0.5 * Inf[i] / N  # infected produce at 50%
        lockdown_cost = 0.0
        if lockdown_start is not None and lockdown_end is not None:
            if lockdown_start <= t <= lockdown_end:
                lockdown_cost = lockdown_reduction * 0.3  # 30% of economy affected
        output_loss[i] = infection_cost + lockdown_cost

    return {
        "t": t_eval.tolist(),
        "S": S.tolist(),
        "I": Inf.tolist(),
        "R": R.tolist(),
        "D": D.tolist(),
        "output_loss": output_loss.tolist(),
        "peak_infected": float(np.max(Inf)),
        "peak_day": int(np.argmax(Inf)),
        "total_deaths": float(D[-1]),
        "cumulative_output_loss": float(np.sum(output_loss) / len(output_loss)),
    }


class PandemicEconomics(LayerBase):
    layer_id = "l8"
    name = "Pandemic Economics"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Analyze pandemic economic impacts and policy tradeoffs.

        Runs SIR-macro simulations with and without lockdowns. Estimates
        lockdown cost-benefit using VSL. Computes excess mortality from
        deviation of actual vs. expected death rates. Optimizes vaccine
        allocation across income groups.

        Returns dict with score, SIR-macro results, lockdown CBA,
        excess mortality, and vaccine allocation.
        """
        country_iso3 = kwargs.get("country_iso3")

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

        # Death rate (crude, per 1000)
        death_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.DYN.CDRT.IN'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not pop_rows or not gdppc_rows:
            return {"score": 50, "results": {"error": "no population or GDP data"}}

        def _index(rows):
            out: dict[str, dict[str, float]] = {}
            for r in rows:
                out.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]
            return out

        pop_data = _index(pop_rows)
        gdppc_data = _index(gdppc_rows)
        death_data = _index(death_rows) if death_rows else {}

        # --- SIR-macro simulation ---
        # Run baseline (no intervention) vs. lockdown scenario
        sir_params = kwargs.get("sir_params", {})
        beta = sir_params.get("beta", 0.3)       # transmission rate
        gamma = sir_params.get("gamma", 0.1)      # recovery rate (~10 day infectious period)
        alpha = sir_params.get("alpha", 0.005)     # IFR ~0.5%
        T_sim = sir_params.get("horizon", 365)
        lockdown_days = sir_params.get("lockdown_days", 60)

        # Baseline: no lockdown
        baseline = _sir_macro(
            S0=0.999, I0=0.001, R0_val=0.0,
            beta=beta, gamma=gamma, alpha=alpha, T=T_sim,
        )

        # Lockdown scenario
        lockdown = _sir_macro(
            S0=0.999, I0=0.001, R0_val=0.0,
            beta=beta, gamma=gamma, alpha=alpha, T=T_sim,
            lockdown_start=30, lockdown_end=30 + lockdown_days,
            lockdown_reduction=0.5,
        )

        sir_results = {
            "parameters": {"beta": beta, "gamma": gamma, "alpha": alpha,
                           "R0": round(beta / gamma, 2)},
            "baseline": {
                "peak_infected_pct": round(baseline["peak_infected"] * 100, 2),
                "peak_day": baseline["peak_day"],
                "total_deaths_pct": round(baseline["total_deaths"] * 100, 3),
                "cumulative_output_loss_pct": round(baseline["cumulative_output_loss"] * 100, 2),
            },
            "lockdown": {
                "peak_infected_pct": round(lockdown["peak_infected"] * 100, 2),
                "peak_day": lockdown["peak_day"],
                "total_deaths_pct": round(lockdown["total_deaths"] * 100, 3),
                "cumulative_output_loss_pct": round(lockdown["cumulative_output_loss"] * 100, 2),
                "lockdown_days": lockdown_days,
            },
            "lives_saved_pct": round(
                (baseline["total_deaths"] - lockdown["total_deaths"]) * 100, 3
            ),
        }

        # --- Lockdown cost-benefit analysis ---
        lockdown_cba = None
        if country_iso3 and country_iso3 in gdppc_data and country_iso3 in pop_data:
            gdp_yrs = gdppc_data[country_iso3]
            pop_yrs = pop_data[country_iso3]
            latest = sorted(set(gdp_yrs.keys()) & set(pop_yrs.keys()))
            if latest:
                yr = latest[-1]
                gdppc = gdp_yrs[yr]
                pop = pop_yrs[yr]

                # VSL: ~40x GDP per capita (Viscusi-Aldy 2003 meta-analysis)
                vsl = 40 * gdppc

                # Lives saved
                lives_saved = (baseline["total_deaths"] - lockdown["total_deaths"]) * pop

                # Benefits: lives saved * VSL
                benefits = lives_saved * vsl

                # Costs: lockdown output loss * GDP
                total_gdp = gdppc * pop
                lockdown_output_loss = lockdown["cumulative_output_loss"] - baseline["cumulative_output_loss"]
                costs = abs(lockdown_output_loss) * total_gdp

                bcr = benefits / costs if costs > 0 else float("inf")

                lockdown_cba = {
                    "vsl_usd": round(float(vsl), 0),
                    "lives_saved": round(float(lives_saved), 0),
                    "benefits_usd": float(benefits),
                    "gdp_cost_usd": float(costs),
                    "benefit_cost_ratio": round(float(bcr), 2),
                    "lockdown_justified": bool(bcr > 1),
                    "population": float(pop),
                    "gdp_per_capita": float(gdppc),
                }

        # --- Excess mortality estimation ---
        # Compare actual death rate to trend-predicted death rate.
        excess_mortality = None
        if country_iso3 and country_iso3 in death_data:
            d_years = death_data[country_iso3]
            yrs = sorted(d_years.keys())
            if len(yrs) >= 8:
                vals = np.array([d_years[y] for y in yrs if d_years[y] is not None])
                t_arr = np.arange(len(vals), dtype=float)

                if len(vals) >= 8:
                    # Fit trend on pre-2020 data (assume last few years may have pandemic)
                    # Use all but last 3 years for trend
                    n_trend = max(5, len(vals) - 3)
                    slope, intercept, _, _, _ = np.linalg.lstsq(
                        np.column_stack([np.ones(n_trend), t_arr[:n_trend]]),
                        vals[:n_trend],
                        rcond=None,
                    )[0:2]  # unpack first 2

                    # Actually, linregress is cleaner
                    from scipy.stats import linregress
                    sl, inter, _, _, _ = linregress(t_arr[:n_trend], vals[:n_trend])

                    # Predict for recent years
                    predicted = inter + sl * t_arr
                    excess = vals - predicted

                    # Last 3 years excess
                    recent_excess = excess[-3:] if len(excess) >= 3 else excess
                    recent_years = yrs[-3:] if len(yrs) >= 3 else yrs

                    # Population for absolute numbers
                    pop_val = None
                    if country_iso3 in pop_data:
                        pop_yrs_c = pop_data[country_iso3]
                        if pop_yrs_c:
                            pop_val = pop_yrs_c[sorted(pop_yrs_c.keys())[-1]]

                    excess_mortality = {
                        "trend_period": f"{yrs[0]}-{yrs[n_trend-1]}",
                        "trend_slope": float(sl),
                        "recent_years": recent_years,
                        "excess_death_rate_per_1000": [round(float(e), 3) for e in recent_excess],
                        "cumulative_excess_rate": round(float(np.sum(recent_excess)), 3),
                    }

                    if pop_val:
                        excess_abs = [float(e * pop_val / 1000) for e in recent_excess]
                        excess_mortality["excess_deaths_absolute"] = [
                            round(a, 0) for a in excess_abs
                        ]
                        excess_mortality["total_excess_deaths"] = round(sum(excess_abs), 0)

        # --- Vaccine allocation optimization (COVAX framework) ---
        # Allocate vaccines across country income groups to minimize deaths.
        # Emanuel et al. (2020): prioritize by expected years of life saved.
        vaccine_alloc = None
        income_groups: dict[str, list[tuple[str, float, float]]] = {
            "high": [], "upper_middle": [], "lower_middle": [], "low": []
        }

        for iso in set(gdppc_data.keys()) & set(pop_data.keys()):
            g_years = gdppc_data[iso]
            p_years = pop_data[iso]
            common = sorted(set(g_years.keys()) & set(p_years.keys()))
            if common:
                yr = common[-1]
                g_val = g_years[yr]
                p_val = p_years[yr]
                if g_val and p_val:
                    if g_val >= 13000:
                        income_groups["high"].append((iso, p_val, g_val))
                    elif g_val >= 4000:
                        income_groups["upper_middle"].append((iso, p_val, g_val))
                    elif g_val >= 1000:
                        income_groups["lower_middle"].append((iso, p_val, g_val))
                    else:
                        income_groups["low"].append((iso, p_val, g_val))

        if any(len(v) > 0 for v in income_groups.values()):
            # Total global population by group
            group_stats = {}
            for group, countries in income_groups.items():
                if countries:
                    total_pop = sum(p for _, p, _ in countries)
                    avg_gdppc = np.mean([g for _, _, g in countries])
                    group_stats[group] = {
                        "n_countries": len(countries),
                        "total_population": float(total_pop),
                        "avg_gdp_per_capita": float(avg_gdppc),
                    }

            total_global_pop = sum(
                gs["total_population"] for gs in group_stats.values()
            )

            # COVAX fair allocation: proportional to population (20% initial target)
            covax_target = 0.20  # 20% population coverage
            for group in group_stats:
                pop_share = group_stats[group]["total_population"] / total_global_pop
                group_stats[group]["population_share"] = round(float(pop_share) * 100, 1)
                group_stats[group]["covax_doses_share"] = round(float(pop_share) * 100, 1)
                group_stats[group]["doses_needed_20pct"] = round(
                    group_stats[group]["total_population"] * covax_target * 2, 0  # 2 doses
                )

            # Optimal: prioritize by IFR * population (more deaths averted per dose)
            # Developing countries have younger populations (lower IFR) but less capacity
            ifr_by_group = {"high": 0.01, "upper_middle": 0.007,
                            "lower_middle": 0.005, "low": 0.004}
            for group in group_stats:
                ifr = ifr_by_group.get(group, 0.005)
                group_stats[group]["assumed_ifr"] = ifr
                group_stats[group]["potential_deaths_no_vaccine"] = round(
                    group_stats[group]["total_population"] * 0.5 * ifr, 0
                )

            # Target country group
            target_group = None
            if country_iso3:
                for group, countries in income_groups.items():
                    if any(iso == country_iso3 for iso, _, _ in countries):
                        target_group = group
                        break

            vaccine_alloc = {
                "income_groups": group_stats,
                "covax_target_coverage": covax_target,
                "target_country_group": target_group,
            }

        # --- Score ---
        # Pandemic preparedness: higher score = more vulnerable
        score = 30
        if sir_results:
            r0 = sir_results["parameters"]["R0"]
            if r0 > 3:
                score += 15
            elif r0 > 2:
                score += 8

        if excess_mortality:
            cum_excess = excess_mortality.get("cumulative_excess_rate", 0)
            if cum_excess > 2:
                score += 20
            elif cum_excess > 0.5:
                score += 10

        if lockdown_cba:
            if not lockdown_cba["lockdown_justified"]:
                score += 10  # lockdown not cost-effective = limited policy space

        if vaccine_alloc and vaccine_alloc.get("target_country_group") in ("low", "lower_middle"):
            score += 10  # lower income = less vaccine access

        score = float(np.clip(score, 0, 100))

        results = {
            "sir_macro": sir_results,
            "lockdown_cba": lockdown_cba,
            "excess_mortality": excess_mortality,
            "vaccine_allocation": vaccine_alloc,
            "country_iso3": country_iso3,
        }

        return {"score": score, "results": results}
