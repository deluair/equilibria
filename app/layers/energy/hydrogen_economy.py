"""Hydrogen economy: green vs grey cost curves, electrolyzer learning, infrastructure, trade.

Methodology
-----------
**Green vs Grey Hydrogen Cost Curves**:
Levelized cost of hydrogen (LCOH) captures full-cycle cost:

    LCOH_grey  = (gas_price * heat_rate + carbon_price * emissions_factor + fixed_opex) / efficiency
    LCOH_green = (capex_electrolyzer * CRF + opex_fixed) / (capacity_factor * hours_yr) + elec_price / efficiency_electrolyzer

Capital recovery factor:
    CRF = r * (1+r)^n / [(1+r)^n - 1]

where r = discount rate, n = project lifetime.

**Electrolyzer Learning Rate (Wright's Law)**:
Cost declines with cumulative capacity according to:

    C(X) = C_0 * (X / X_0)^(-alpha)
    alpha = log(1 - LR) / log(2)

where:
    LR   = learning rate (fraction cost falls per doubling of cumulative capacity)
    X    = cumulative installed capacity
    C_0  = reference cost at X_0

Typical electrolyzer LR: 15-20% (PEM), 10-15% (alkaline).

**Infrastructure Investment Needs**:
    investment_gap = required_infrastructure - current_deployed
    pipeline_cost  = distance_km * cost_per_km_pipeline (capex ~$1-3M/km)
    storage_cost   = storage_twh * cost_per_twh (salt cavern ~$2-5/kg, ~$60-150M/TWh_H2)

**Trade Potential (gravity approximation)**:
    export_potential = production_capacity - domestic_demand
    transport_cost   = distance * shipping_cost_per_tonne_km (liquefied H2 ~$0.2-0.4/kg/1000km)
    competitiveness  = LCOH + transport_cost vs destination market price

Score: high green premium over grey + slow learning + large infrastructure gap -> STRESS.
Parity achieved with strong learning progress and trade viability -> STABLE.

Sources: IEA Hydrogen, IRENA Green Hydrogen Cost Outlook, BloombergNEF Hydrogen Economy Outlook
"""

import numpy as np
from scipy import optimize

from app.layers.base import LayerBase


def _capital_recovery_factor(r: float, n: int) -> float:
    """CRF = r*(1+r)^n / [(1+r)^n - 1]."""
    if r <= 0:
        return 1.0 / n
    return r * (1 + r) ** n / ((1 + r) ** n - 1)


def _wright_learning(c0: float, x0: float, x_new: float, lr: float) -> float:
    """Cost at cumulative capacity x_new given learning rate lr."""
    if x0 <= 0 or x_new <= 0 or lr <= 0 or lr >= 1:
        return c0
    alpha = -np.log2(1 - lr)
    return c0 * (x_new / x0) ** (-alpha)


class HydrogenEconomy(LayerBase):
    layer_id = "l16"
    name = "Hydrogen Economy"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")

        series_map = {
            "gas_price": f"NATURAL_GAS_PRICE_{country}",
            "carbon_price": f"CARBON_PRICE_{country}",
            "electricity_price": f"ELECTRICITY_PRICE_INDUSTRIAL_{country}",
            "electrolyzer_capex": f"ELECTROLYZER_CAPEX_{country}",
            "electrolyzer_capacity": f"ELECTROLYZER_CAPACITY_GW_{country}",
            "green_h2_production": f"GREEN_H2_PRODUCTION_MT_{country}",
            "grey_h2_production": f"GREY_H2_PRODUCTION_MT_{country}",
            "h2_demand_forecast": f"H2_DEMAND_FORECAST_MT_{country}",
            "renewable_capex": f"RENEWABLE_CAPEX_{country}",
            "capacity_factor_solar": f"CAPACITY_FACTOR_SOLAR_{country}",
            "h2_pipeline_km": f"H2_PIPELINE_KM_{country}",
            "h2_storage_capacity": f"H2_STORAGE_CAPACITY_MT_{country}",
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

        # --- LCOH: Grey hydrogen ---
        grey_lcoh = None
        if "gas_price" in data:
            gas_vals = list(data["gas_price"].values())
            gas = float(gas_vals[-1]) if gas_vals else 5.0  # $/MMBtu
            # Steam methane reforming: heat rate ~160 MMBtu/t H2, ~9-10 t CO2/t H2
            carbon = 0.0
            if "carbon_price" in data:
                c_vals = list(data["carbon_price"].values())
                carbon = float(c_vals[-1]) if c_vals else 0.0
            heat_rate = 160.0  # MMBtu/tonne H2
            co2_factor = 9.5   # tonne CO2 / tonne H2
            fixed_opex = 0.15  # $/kg (maintenance, labor)
            grey_lcoh = gas * heat_rate / 1000 + carbon * co2_factor / 1000 + fixed_opex
            results["grey_h2_lcoh_per_kg"] = round(float(grey_lcoh), 3)

        # --- LCOH: Green hydrogen ---
        green_lcoh = None
        if "electricity_price" in data:
            elec_vals = list(data["electricity_price"].values())
            elec_price = float(elec_vals[-1]) if elec_vals else 0.05  # $/kWh

            # Electrolyzer capex ($/kW)
            capex = 800.0  # default $/kW (current PEM ~$600-1200)
            if "electrolyzer_capex" in data:
                cap_vals = list(data["electrolyzer_capex"].values())
                capex = float(cap_vals[-1]) if cap_vals else capex

            efficiency_kwh_per_kg = 55.0  # kWh/kg H2 (PEM ~50-70)
            capacity_factor = 0.45  # default
            if "capacity_factor_solar" in data:
                cf_vals = list(data["capacity_factor_solar"].values())
                capacity_factor = float(cf_vals[-1]) if cf_vals else capacity_factor

            r = 0.08
            n = 20
            crf = _capital_recovery_factor(r, n)
            hours_yr = 8760.0
            opex_fixed_pct = 0.02
            # Annualized capex per kW
            annualized_capex = capex * crf + capex * opex_fixed_pct
            # Production per kW per year (kg)
            prod_per_kw = capacity_factor * hours_yr / efficiency_kwh_per_kg
            capex_per_kg = annualized_capex / prod_per_kw if prod_per_kw > 0 else 9999
            elec_per_kg = elec_price * efficiency_kwh_per_kg
            green_lcoh = capex_per_kg + elec_per_kg
            results["green_h2_lcoh_per_kg"] = round(float(green_lcoh), 3)
            results["green_lcoh_breakdown"] = {
                "capex_component_per_kg": round(float(capex_per_kg), 3),
                "electricity_component_per_kg": round(float(elec_per_kg), 3),
                "capacity_factor": round(capacity_factor, 3),
                "electrolyzer_capex_per_kw": round(capex, 1),
            }

        if grey_lcoh is not None and green_lcoh is not None:
            green_premium = green_lcoh - grey_lcoh
            parity = green_premium <= 0
            results["green_grey_premium_per_kg"] = round(float(green_premium), 3)
            results["parity_achieved"] = parity

        # --- Electrolyzer learning rate ---
        if "electrolyzer_capex" in data and len(data["electrolyzer_capex"]) >= 4:
            dates_sorted = sorted(data["electrolyzer_capex"].keys())
            costs = np.array([data["electrolyzer_capex"][d] for d in dates_sorted])

            # Estimate learning rate via log-linear regression on cumulative capacity
            if "electrolyzer_capacity" in data:
                cum_cap = []
                total = 0.0
                for d in dates_sorted:
                    if d in data["electrolyzer_capacity"]:
                        total += data["electrolyzer_capacity"][d]
                    cum_cap.append(max(total, 1.0))
                cum_cap = np.array(cum_cap)
            else:
                # Proxy: exponential growth from year
                years = np.array([float(d[:4]) for d in dates_sorted])
                cum_cap = np.exp((years - years[0]) * 0.3) + 1.0

            valid = costs > 0
            if np.sum(valid) >= 4:
                log_cap = np.log(cum_cap[valid])
                log_cost = np.log(costs[valid])
                slope, intercept = np.polyfit(log_cap, log_cost, 1)
                lr_est = 1 - 2 ** slope  # learning rate
                # Forecast to 100 GW cumulative
                target_cum = 100.0  # GW
                current_cum = float(cum_cap[-1])
                forecast_cost = np.exp(intercept) * target_cum ** slope
                results["learning_curve"] = {
                    "estimated_learning_rate": round(float(lr_est), 4),
                    "current_capex_per_kw": round(float(costs[-1]), 1),
                    "forecast_capex_at_100gw": round(float(forecast_cost), 1),
                    "current_cumulative_gw": round(current_cum, 2),
                    "log_log_slope": round(float(slope), 4),
                }

        # --- Infrastructure investment needs ---
        if "h2_demand_forecast" in data:
            demand_vals = list(data["h2_demand_forecast"].values())
            demand_mt = float(demand_vals[-1]) if demand_vals else 0
            green_prod = 0.0
            if "green_h2_production" in data:
                gp_vals = list(data["green_h2_production"].values())
                green_prod = float(gp_vals[-1]) if gp_vals else 0
            production_gap_mt = max(demand_mt - green_prod, 0)
            # Rough electrolyzer capacity needed: ~180 kg/kW/yr at 45% CF
            needed_gw = production_gap_mt * 1e9 / (0.45 * 8760 / 55 * 1e6) if production_gap_mt > 0 else 0
            # Investment: $800/kW average
            capex_usd = needed_gw * 1e9 * 800 / 1e9  # $bn

            pipeline_km = 0.0
            if "h2_pipeline_km" in data:
                pip_vals = list(data["h2_pipeline_km"].values())
                pipeline_km = float(pip_vals[-1]) if pip_vals else 0
            # Cost per km for new dedicated H2 pipeline ~$2M/km
            pipeline_needed_km = max(needed_gw * 200 - pipeline_km, 0)
            pipeline_cost_bn = pipeline_needed_km * 2e6 / 1e9

            results["infrastructure_needs"] = {
                "demand_mt": round(demand_mt, 2),
                "green_production_mt": round(green_prod, 2),
                "production_gap_mt": round(production_gap_mt, 2),
                "electrolyzer_capacity_needed_gw": round(float(needed_gw), 2),
                "electrolyzer_investment_bn_usd": round(float(capex_usd), 1),
                "pipeline_investment_bn_usd": round(float(pipeline_cost_bn), 1),
                "total_infrastructure_investment_bn_usd": round(
                    float(capex_usd + pipeline_cost_bn), 1
                ),
            }

        # --- Trade potential ---
        grey_prod = 0.0
        if "grey_h2_production" in data:
            gv = list(data["grey_h2_production"].values())
            grey_prod = float(gv[-1]) if gv else 0
        total_prod = float(
            (list(data["green_h2_production"].values())[-1] if "green_h2_production" in data else 0)
            + grey_prod
        )
        dom_demand = float(
            list(data["h2_demand_forecast"].values())[-1]
            if "h2_demand_forecast" in data else total_prod
        )
        export_potential_mt = max(total_prod - dom_demand, 0)
        if total_prod > 0:
            results["trade_potential"] = {
                "total_production_mt": round(total_prod, 3),
                "domestic_demand_mt": round(dom_demand, 3),
                "export_potential_mt": round(export_potential_mt, 3),
                "net_exporter": export_potential_mt > 0,
            }

        # --- Score ---
        score = 30.0

        # Green-grey premium: larger premium = higher stress
        if grey_lcoh is not None and green_lcoh is not None:
            premium = green_lcoh - grey_lcoh
            if premium > 0:
                score += min(premium * 15, 35)  # $2/kg premium -> +30 pts
            else:
                score -= min(abs(premium) * 10, 20)  # parity achieved -> relief

        # Learning rate: slow learning = higher risk
        lr_info = results.get("learning_curve", {})
        lr = lr_info.get("estimated_learning_rate", 0.15)
        if lr < 0.10:
            score += 15
        elif lr > 0.20:
            score -= 10

        # Infrastructure gap
        infra = results.get("infrastructure_needs", {})
        total_inv = infra.get("total_infrastructure_investment_bn_usd", 0) or 0
        if total_inv > 100:
            score += min(total_inv / 50, 20)

        score = float(np.clip(score, 0, 100))
        return {"score": round(score, 1), "results": results}
