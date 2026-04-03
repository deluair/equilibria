"""Energy storage: battery cost decline, grid storage value, V2G economics, pumped hydro vs Li-ion.

Methodology
-----------
**Battery Cost Decline Curves (Wright's Law)**:
    C(X) = C_0 * (X / X_0)^(-alpha), alpha = log(1-LR)/log(2)

    Lithium-ion (utility): LR ~18-20%, from $1200/kWh (2010) to ~$130/kWh (2024).
    Target for grid parity: ~$75-100/kWh.
    Cumulative volume doubling every 2-3 years at current pace.

**Grid Storage Value Estimation**:
    Energy arbitrage value:
        V_arb = (peak_price - off_peak_price) * cycles_yr * round_trip_efficiency
    Capacity value (avoiding peaker plant):
        V_cap = capacity_credit * peaker_cost_per_kw_yr
    Ancillary services (frequency regulation, reserves):
        V_anc = reserve_price * availability_hours
    Total value = V_arb + V_cap + V_anc

    LCOS (Levelized Cost of Storage):
        LCOS = (capex * CRF + opex) / (annual_discharge_MWh)
        annual_discharge_MWh = capacity_kWh * cycles_yr * RTE / 1000

    Grid parity: LCOS <= total storage value.

**Vehicle-to-Grid (V2G) Economics**:
    Revenue per EV per year:
        R_v2g = min(battery_capacity_kWh * DoD, grid_demand_kWh_per_ev)
                * (peak_price - off_peak_price) * utilization_factor
    Battery degradation cost:
        D = cycles_additional * degradation_rate * replacement_cost_per_kWh
    Net V2G benefit = R_v2g - D - grid_operator_share

**Pumped Hydro vs Li-ion Comparison**:
    Pumped hydro:   $800-2000/kWh capital, 40-80yr life, 75-80% RTE, unlimited cycles
    Li-ion:         $100-200/kWh capital, 15-20yr life, 85-92% RTE, 3000-6000 cycles

    LCOS comparison over 20-yr period (common accounting horizon).
    Geographical constraint on pumped hydro: suitable sites limited.

Score: high LCOS with no grid parity + slow cost decline + V2G uneconomic -> STRESS.
Cost below grid value + rapid learning + V2G viable -> STABLE.

Sources: BloombergNEF, NREL ATB, Rocky Mountain Institute, DOE/EERE
"""

import numpy as np

from app.layers.base import LayerBase


def _capital_recovery_factor(r: float, n: int) -> float:
    if r <= 0:
        return 1.0 / n
    return r * (1 + r) ** n / ((1 + r) ** n - 1)


def _wright_cost(c0: float, x0: float, x_new: float, lr: float) -> float:
    if x0 <= 0 or x_new <= 0 or lr <= 0 or lr >= 1:
        return c0
    alpha = -np.log2(1 - lr)
    return c0 * (x_new / x0) ** (-alpha)


class EnergyStorage(LayerBase):
    layer_id = "l16"
    name = "Energy Storage"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")

        series_map = {
            "battery_capex_kwh": f"BATTERY_CAPEX_KWH_{country}",
            "battery_capacity_gwh": f"BATTERY_CAPACITY_GWH_{country}",
            "grid_storage_gwh": f"GRID_STORAGE_GWH_{country}",
            "peak_electricity_price": f"ELECTRICITY_PRICE_PEAK_{country}",
            "offpeak_electricity_price": f"ELECTRICITY_PRICE_OFFPEAK_{country}",
            "peaker_plant_cost": f"PEAKER_PLANT_COST_KW_{country}",
            "reserve_price": f"ANCILLARY_RESERVE_PRICE_{country}",
            "ev_fleet_size": f"EV_FLEET_SIZE_{country}",
            "ev_battery_kwh": f"EV_BATTERY_SIZE_KWH_{country}",
            "pumped_hydro_capacity_gwh": f"PUMPED_HYDRO_CAPACITY_GWH_{country}",
            "pumped_hydro_capex": f"PUMPED_HYDRO_CAPEX_KWH_{country}",
            "renewable_curtailment_twh": f"RENEWABLE_CURTAILMENT_TWH_{country}",
            "battery_cumulative_gwh": f"BATTERY_CUMULATIVE_GWH_GLOBAL",
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

        r_disc = 0.08

        # --- Battery cost decline (learning curve) ---
        li_ion_lr = 0.19  # Historical ~19% learning rate
        battery_cost = None
        if "battery_capex_kwh" in data:
            batt_vals = list(data["battery_capex_kwh"].values())
            battery_cost = float(batt_vals[-1]) if batt_vals else 130.0

            cum_gwh = 0.0
            if "battery_cumulative_gwh" in data:
                cum_vals = list(data["battery_cumulative_gwh"].values())
                cum_gwh = float(cum_vals[-1]) if cum_vals else 0
            elif "battery_capacity_gwh" in data:
                # Approximate cumulative from annual series
                cap_vals = list(data["battery_capacity_gwh"].values())
                cum_gwh = sum(cap_vals)

            # Estimate learning rate from time series if 4+ points
            if len(batt_vals) >= 4:
                costs_arr = np.array(batt_vals)
                n_pts = len(costs_arr)
                # Proxy cum volume: double every 3 years
                cum_proxy = np.array([1.0 * (2 ** (i / 3)) for i in range(n_pts)])
                valid = costs_arr > 0
                if np.sum(valid) >= 4:
                    slope, _ = np.polyfit(np.log(cum_proxy[valid]), np.log(costs_arr[valid]), 1)
                    li_ion_lr = 1 - 2 ** slope

            # Forecast to grid parity (~$75/kWh) using current LR
            c0 = battery_cost
            x0 = max(cum_gwh, 1.0)
            parity_target = 75.0  # $/kWh
            alpha = -np.log2(1 - li_ion_lr)
            if alpha > 0 and c0 > parity_target:
                # Solve: c0 * (x / x0)^(-alpha) = parity_target
                x_parity = x0 * (c0 / parity_target) ** (1 / alpha)
                doublings_to_parity = np.log2(x_parity / x0)
            else:
                x_parity = x0
                doublings_to_parity = 0

            results["battery_learning_curve"] = {
                "current_cost_per_kwh": round(float(battery_cost), 1),
                "estimated_learning_rate": round(float(li_ion_lr), 4),
                "cumulative_gwh": round(float(cum_gwh), 1),
                "parity_target_per_kwh": round(parity_target, 1),
                "doublings_to_parity": round(float(doublings_to_parity), 2),
                "at_parity": battery_cost <= parity_target,
            }

        # --- Grid storage value estimation ---
        peak_price = None
        if "peak_electricity_price" in data and "offpeak_electricity_price" in data:
            peak_vals = list(data["peak_electricity_price"].values())
            offpeak_vals = list(data["offpeak_electricity_price"].values())
            peak_price = float(peak_vals[-1]) if peak_vals else 0.12  # $/kWh
            offpeak_price = float(offpeak_vals[-1]) if offpeak_vals else 0.04

            spread = max(peak_price - offpeak_price, 0)
            cycles_yr = 300
            rte = 0.88  # Li-ion round-trip efficiency
            v_arb = spread * cycles_yr * rte  # $/kWh/yr

            v_cap = 0.0
            if "peaker_plant_cost" in data:
                peaker_vals = list(data["peaker_plant_cost"].values())
                peaker_cost = float(peaker_vals[-1]) if peaker_vals else 150.0  # $/kW
                capacity_credit = 0.80  # 80% capacity credit for 4-hr battery
                v_cap = peaker_cost * capacity_credit / (4 * 1)  # convert $/kW to $/kWh-capacity

            v_anc = 0.0
            if "reserve_price" in data:
                res_vals = list(data["reserve_price"].values())
                reserve_price = float(res_vals[-1]) if res_vals else 5.0  # $/MW-hr
                v_anc = reserve_price * 4000 / 1000  # ~4000 hrs * $/MW-hr -> $/kW/yr -> $/kWh

            total_value = v_arb + v_cap + v_anc

            # LCOS
            capex_kwh = battery_cost if battery_cost else 130.0
            n_batt = 15  # battery life years
            crf = _capital_recovery_factor(r_disc, n_batt)
            opex_pct = 0.015
            annual_discharge = cycles_yr * rte  # kWh per kWh capacity per year
            lcos = ((capex_kwh * crf + capex_kwh * opex_pct) / annual_discharge) if annual_discharge > 0 else 9999

            results["grid_storage_value"] = {
                "price_spread_per_kwh": round(float(spread), 4),
                "arbitrage_value_per_kwh_yr": round(float(v_arb), 2),
                "capacity_value_per_kwh_yr": round(float(v_cap), 2),
                "ancillary_value_per_kwh_yr": round(float(v_anc), 2),
                "total_value_per_kwh_yr": round(float(total_value), 2),
                "lcos_per_kwh": round(float(lcos), 3),
                "grid_parity": lcos <= total_value,
            }

        # --- V2G economics ---
        if "ev_fleet_size" in data:
            ev_vals = list(data["ev_fleet_size"].values())
            ev_fleet = float(ev_vals[-1]) if ev_vals else 0

            ev_battery = 60.0  # kWh default
            if "ev_battery_kwh" in data:
                eb_vals = list(data["ev_battery_kwh"].values())
                ev_battery = float(eb_vals[-1]) if eb_vals else ev_battery

            dod_v2g = 0.20  # Discharge 20% of battery for grid
            utilization = 0.25  # 25% of fleet available at peak
            spread = (peak_price - (list(data["offpeak_electricity_price"].values())[-1]
                      if "offpeak_electricity_price" in data else 0.04)) if peak_price else 0.05
            rev_per_ev_yr = ev_battery * dod_v2g * spread * 365 * utilization

            # Degradation cost: each V2G discharge cycle costs ~$0.05/kWh in battery life
            degrad_cost_per_ev = ev_battery * dod_v2g * 0.05 * 365 * utilization
            net_v2g_per_ev = rev_per_ev_yr - degrad_cost_per_ev

            total_v2g_revenue_bn = ev_fleet * net_v2g_per_ev / 1e9

            results["v2g_economics"] = {
                "ev_fleet_size": round(ev_fleet, 0),
                "revenue_per_ev_per_yr": round(float(rev_per_ev_yr), 1),
                "degradation_cost_per_ev_yr": round(float(degrad_cost_per_ev), 1),
                "net_v2g_benefit_per_ev_yr": round(float(net_v2g_per_ev), 1),
                "total_fleet_revenue_bn_usd": round(float(total_v2g_revenue_bn), 3),
                "v2g_economic": net_v2g_per_ev > 0,
            }

        # --- Pumped hydro vs Li-ion comparison ---
        ph_capex = 1500.0  # $/kWh default
        if "pumped_hydro_capex" in data:
            ph_vals = list(data["pumped_hydro_capex"].values())
            ph_capex = float(ph_vals[-1]) if ph_vals else ph_capex

        # 20-year LCOS comparison (common horizon)
        n_compare = 20
        crf_ph = _capital_recovery_factor(0.06, 60)  # pumped hydro over 60yr
        crf_li = _capital_recovery_factor(0.08, 15)  # Li-ion over 15yr, 2 replacements in 30yr
        # Annualize over 20 common years
        cycles_yr = 300
        rte_ph = 0.77
        rte_li = 0.88

        li_capex = battery_cost if battery_cost else 130.0
        lcos_ph = (ph_capex * crf_ph) / (cycles_yr * rte_ph) if cycles_yr * rte_ph > 0 else 9999
        lcos_li = (li_capex * crf_li) / (cycles_yr * rte_li) if cycles_yr * rte_li > 0 else 9999

        results["pumped_hydro_vs_lion"] = {
            "pumped_hydro_capex_per_kwh": round(float(ph_capex), 0),
            "lion_capex_per_kwh": round(float(li_capex), 1),
            "pumped_hydro_lcos": round(float(lcos_ph), 4),
            "lion_lcos": round(float(lcos_li), 4),
            "pumped_hydro_cheaper": lcos_ph < lcos_li,
            "pumped_hydro_capacity_gwh": round(
                float(list(data["pumped_hydro_capacity_gwh"].values())[-1]), 1
            ) if "pumped_hydro_capacity_gwh" in data else None,
        }

        # --- Score ---
        score = 20.0

        # LCOS vs grid parity
        gs_info = results.get("grid_storage_value", {})
        if gs_info:
            if not gs_info.get("grid_parity"):
                lcos_v = gs_info.get("lcos_per_kwh", 0.15)
                total_v = gs_info.get("total_value_per_kwh_yr", 0.10)
                if total_v > 0:
                    parity_gap = (lcos_v - total_v) / total_v
                    score += min(parity_gap * 30, 30)
            else:
                score -= 10

        # Battery learning curve progress
        bl_info = results.get("battery_learning_curve", {})
        if bl_info:
            doublings = bl_info.get("doublings_to_parity", 0) or 0
            score += min(doublings * 3, 20)

        # Curtailment (storage urgency)
        if "renewable_curtailment_twh" in data:
            curt_vals = list(data["renewable_curtailment_twh"].values())
            curtailment = float(curt_vals[-1]) if curt_vals else 0
            score += min(curtailment * 2, 15)

        score = float(np.clip(score, 0, 100))
        return {"score": round(score, 1), "results": results}
