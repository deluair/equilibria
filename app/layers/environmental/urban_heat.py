"""Urban heat island economics: UHI intensity, cooling costs, heat mortality, green ROI.

Estimates urban heat island (UHI) intensity from urbanization and climate data.
Computes the additional energy cost of cooling attributable to UHI. Estimates
excess mortality from extreme heat events using a dose-response framework.
Performs cost-benefit analysis of green infrastructure interventions.

Key references:
    Oke, T.R. (1973). City size and the urban heat island. Atmospheric
        Environment, 7(8), 769-779.
    Santamouris, M. (2014). Cooling the cities - a review of reflective and
        green roof mitigation technologies. Solar Energy, 103, 682-703.
    Gasparrini, A. et al. (2017). Projections of temperature-related excess
        mortality under climate change scenarios. Lancet Planet Health, 1(9).
    Akbari, H. & Konopacki, S. (2005). Calculating energy-saving potentials of
        heat-island reduction strategies. Energy Policy, 33(6), 721-756.
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class UrbanHeat(LayerBase):
    layer_id = "l9"
    name = "Urban Heat Island"
    weight = 0.20

    # Oke (1973) UHI scaling: delta_T ~ 2.01 * log10(Pop) - 4.06
    OKE_A = 2.01
    OKE_B = 4.06

    # Cooling energy demand coefficient: each 1C UHI adds ~5-10% cooling load
    COOLING_DEMAND_PER_C = 0.07   # 7% per degree

    # Baseline cooling energy as fraction of total energy (hot climates)
    BASELINE_COOLING_FRACTION = 0.20

    # Mortality dose-response: RR per degree C above threshold (Gasparrini 2017)
    MORTALITY_RR_PER_C = 1.025   # 2.5% increase per degree above threshold

    async def compute(self, db, **kwargs) -> dict:
        """Compute urban heat island economic impacts.

        Fetches urbanization rate, population, GDP, energy use, and temperature
        data. Estimates UHI intensity using Oke scaling. Computes cooling energy
        cost. Estimates heat mortality. Evaluates green infrastructure ROI.

        Returns dict with score, uhi_intensity, cooling_costs, heat_mortality,
        and green_infrastructure_roi.
        """
        country_iso3 = kwargs.get("country_iso3")

        # Urban population (% of total)
        urban_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.URB.TOTL.IN.ZS'
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

        # Energy use per capita (kg oil equivalent)
        energy_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'EG.USE.PCAP.KG.OE'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # CO2 per capita (proxy for climate/heat context)
        co2_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'EN.ATM.CO2E.PC'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not pop_rows and not urban_rows:
            return {"score": 50, "results": {"error": "no urbanization or population data"}}

        def _index(rows):
            out: dict[str, dict[str, float]] = {}
            for r in rows:
                out.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]
            return out

        urban_data = _index(urban_rows) if urban_rows else {}
        pop_data = _index(pop_rows) if pop_rows else {}
        gdppc_data = _index(gdppc_rows) if gdppc_rows else {}
        gdp_data = _index(gdp_rows) if gdp_rows else {}
        energy_data = _index(energy_rows) if energy_rows else {}
        co2_data = _index(co2_rows) if co2_rows else {}

        uhi_intensity = None
        cooling_costs = None
        heat_mortality = None
        green_infrastructure_roi = None

        target = country_iso3
        pop_ts = pop_data.get(target, {}) if target else {}
        urban_ts = urban_data.get(target, {}) if target else {}
        gdppc_ts = gdppc_data.get(target, {}) if target else {}
        gdp_ts = gdp_data.get(target, {}) if target else {}
        energy_ts = energy_data.get(target, {}) if target else {}

        if pop_ts and gdppc_ts:
            latest_yr = sorted(set(pop_ts.keys()) & set(gdppc_ts.keys()))[-1]
            pop = pop_ts[latest_yr]
            gdppc = gdppc_ts[latest_yr]
            gdp_val = gdp_ts.get(latest_yr, gdppc * pop) if gdp_ts else gdppc * pop

            # Urban population
            urban_pct = None
            if urban_ts:
                latest_urban_yr = sorted(urban_ts.keys())[-1]
                urban_pct = urban_ts[latest_urban_yr] / 100.0

            urban_pop = pop * (urban_pct or 0.55)

            # Urbanization trend
            urbanization_trend = None
            if urban_ts and len(urban_ts) >= 5:
                u_yrs = sorted(urban_ts.keys())
                u_vals = np.array([urban_ts[y] for y in u_yrs])
                t_arr = np.arange(len(u_vals), dtype=float)
                sl, _, r, _, _ = linregress(t_arr, u_vals)
                urbanization_trend = {
                    "slope_pct_per_year": round(float(sl), 3),
                    "latest_urban_pct": round(float(u_vals[-1]), 1),
                    "r_squared": round(float(r) ** 2, 3),
                    "direction": "increasing" if sl > 0 else "stable",
                }

            # --- UHI intensity estimation (Oke 1973 scaling) ---
            # Cities are 1-8C warmer than surrounding rural areas
            # Oke: delta_T_max = A * log10(city_population) - B
            # Assume average city = urban_pop / n_cities, where n_cities ~ pop/1M
            n_cities = max(1, pop / 1e6)
            avg_city_pop = urban_pop / n_cities

            if avg_city_pop > 0:
                delta_t_oke = max(
                    0.0,
                    self.OKE_A * np.log10(avg_city_pop) - self.OKE_B,
                )
            else:
                delta_t_oke = 0.0

            # Impervious surface amplification: higher income = more concrete
            impervious_factor = 1.0 + (min(urban_pct or 0.55, 0.90) - 0.40) * 0.5
            uhi_delta_t = delta_t_oke * impervious_factor

            uhi_intensity = {
                "year": latest_yr,
                "urban_population_pct": round((urban_pct or 0.55) * 100, 1),
                "urban_population": round(urban_pop, 0),
                "avg_city_population": round(avg_city_pop, 0),
                "uhi_delta_t_celsius": round(float(uhi_delta_t), 2),
                "uhi_classification": (
                    "intense" if uhi_delta_t > 4
                    else "moderate" if uhi_delta_t > 2
                    else "mild"
                ),
                "urbanization_trend": urbanization_trend,
            }

            # --- Cooling energy costs ---
            energy_per_cap = None
            if energy_ts:
                latest_e_yr = sorted(energy_ts.keys())[-1]
                energy_per_cap = energy_ts[latest_e_yr]

            total_energy_use_kgoe = (energy_per_cap or 1500.0) * pop
            cooling_energy_fraction = self.BASELINE_COOLING_FRACTION * (
                1 + self.COOLING_DEMAND_PER_C * uhi_delta_t
            )
            uhi_extra_cooling_fraction = (
                self.BASELINE_COOLING_FRACTION
                * self.COOLING_DEMAND_PER_C
                * uhi_delta_t
            )

            # Energy price proxy: $0.10-0.25/kWh; convert kgoe -> kWh (1 kgoe = 11.63 kWh)
            kwh_per_kgoe = 11.63
            energy_price_kwh = 0.08 + gdppc / 1e6   # rough income-based price

            uhi_cooling_cost = (
                total_energy_use_kgoe
                * uhi_extra_cooling_fraction
                * kwh_per_kgoe
                * energy_price_kwh
            )
            cooling_cost_pct_gdp = uhi_cooling_cost / gdp_val * 100

            cooling_costs = {
                "total_energy_use_kgoe": round(total_energy_use_kgoe, 0),
                "cooling_energy_fraction_pct": round(cooling_energy_fraction * 100, 2),
                "uhi_attributable_cooling_fraction_pct": round(
                    uhi_extra_cooling_fraction * 100, 2
                ),
                "uhi_cooling_cost_musd": round(uhi_cooling_cost / 1e6, 2),
                "cooling_cost_pct_gdp": round(cooling_cost_pct_gdp, 4),
                "energy_price_per_kwh_usd": round(energy_price_kwh, 3),
            }

            # --- Heat mortality estimation ---
            # Gasparrini (2017): RR = 1.025 per degree C above threshold
            # Threshold: ~29C for most regions (country-specific in reality)
            crude_death_rate = 8.0 / 1000  # approximate global avg
            annual_deaths = pop * crude_death_rate
            heat_threshold_c = 29.0

            # Effective excess temperature: UHI contributes to days above threshold
            # Assume UHI extends heat wave duration by 20% per degree
            heat_wave_days_baseline = max(1.0, 5.0 * (gdppc < 15000))
            uhi_additional_days = heat_wave_days_baseline * uhi_delta_t * 0.10

            excess_temp = max(0.0, uhi_delta_t * 0.60)  # UHI contribution above threshold

            # Attributable fraction: RR / (RR + 1) approx
            rr = self.MORTALITY_RR_PER_C ** excess_temp
            attributable_fraction = (rr - 1) / rr

            excess_heat_deaths = annual_deaths * attributable_fraction
            vsl = 40 * gdppc   # value of statistical life (Viscusi-Aldy 2003)
            mortality_economic_cost = excess_heat_deaths * vsl

            heat_mortality = {
                "uhi_contribution_c": round(float(excess_temp), 2),
                "excess_temperature_rr": round(float(rr), 4),
                "attributable_fraction": round(float(attributable_fraction), 4),
                "excess_heat_deaths_annual": round(float(excess_heat_deaths), 0),
                "mortality_economic_cost_musd": round(mortality_economic_cost / 1e6, 2),
                "mortality_cost_pct_gdp": round(mortality_economic_cost / gdp_val * 100, 4),
                "extended_heat_wave_days": round(uhi_additional_days, 1),
            }

            # --- Green infrastructure ROI ---
            # Santamouris (2014): green roofs reduce UHI by 0.3-0.5C; parks by 0.5-3C
            # Cool roofs + urban trees: $1-5 per m2 per year
            urban_area_km2 = urban_pop / 3000.0   # ~3000 people per km2 average
            urban_area_m2 = urban_area_km2 * 1e6

            # Intervention: green roofs on 20% of buildings, urban trees
            green_roof_coverage_pct = 0.20
            green_roof_area_m2 = urban_area_m2 * 0.20 * green_roof_coverage_pct
            tree_canopy_increase_pct = 0.10

            # Costs
            green_roof_cost_per_m2 = 50 * (gdppc / 10000) ** 0.3
            green_roof_total_cost = green_roof_area_m2 * green_roof_cost_per_m2
            tree_program_cost = urban_area_m2 * 0.5 * tree_canopy_increase_pct

            total_gi_cost = green_roof_total_cost + tree_program_cost

            # Benefits: cooling cost reduction + mortality reduction
            # Green roofs reduce UHI by ~0.3C, trees by ~0.5C -> 0.8C total
            uhi_reduction_c = 0.8
            cooling_benefit = (
                total_energy_use_kgoe
                * self.BASELINE_COOLING_FRACTION
                * self.COOLING_DEMAND_PER_C
                * uhi_reduction_c
                * kwh_per_kgoe
                * energy_price_kwh
            )

            # Mortality benefit from UHI reduction
            rr_with_gi = self.MORTALITY_RR_PER_C ** max(0.0, excess_temp - uhi_reduction_c * 0.6)
            af_with_gi = (rr_with_gi - 1) / rr_with_gi
            mortality_benefit = (
                annual_deaths * (attributable_fraction - af_with_gi) * vsl
            )

            total_gi_benefit = cooling_benefit + mortality_benefit

            gi_bcr = total_gi_benefit / total_gi_cost if total_gi_cost > 0 else None
            gi_payback_years = total_gi_cost / (total_gi_benefit / 30) if total_gi_benefit > 0 else None

            green_infrastructure_roi = {
                "green_roof_area_m2": round(green_roof_area_m2, 0),
                "total_gi_investment_musd": round(total_gi_cost / 1e6, 2),
                "annual_cooling_benefit_musd": round(cooling_benefit / 1e6, 2),
                "annual_mortality_benefit_musd": round(mortality_benefit / 1e6, 2),
                "total_annual_benefit_musd": round(total_gi_benefit / 1e6, 2),
                "benefit_cost_ratio": round(float(gi_bcr), 2) if gi_bcr else None,
                "payback_years": round(float(gi_payback_years), 1) if gi_payback_years else None,
                "uhi_reduction_potential_c": round(uhi_reduction_c, 1),
                "investment_justified": bool(gi_bcr and gi_bcr > 1),
            }

        # --- Score ---
        score = 25.0

        if uhi_intensity:
            delta_t = uhi_intensity["uhi_delta_t_celsius"]
            if delta_t > 4:
                score += 30
            elif delta_t > 2.5:
                score += 20
            elif delta_t > 1.5:
                score += 10

        if cooling_costs:
            pct = cooling_costs["cooling_cost_pct_gdp"]
            if pct > 0.5:
                score += 20
            elif pct > 0.2:
                score += 10

        if heat_mortality:
            deaths = heat_mortality["excess_heat_deaths_annual"]
            if deaths > 5000:
                score += 20
            elif deaths > 1000:
                score += 10
            elif deaths > 200:
                score += 5

        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": target,
                "uhi_intensity": uhi_intensity,
                "cooling_costs": cooling_costs,
                "heat_mortality": heat_mortality,
                "green_infrastructure_roi": green_infrastructure_roi,
            },
        }
