"""Carbon capture: CCS cost per ton, EOR economics, DAC scaling, carbon utilization market.

Methodology
-----------
**CCS (Carbon Capture and Storage) Cost**:
Levelized cost of CO2 capture ($/tCO2) from post-combustion:

    LCOC = (capex * CRF + opex_fixed) / annual_capture
           + opex_variable + transport_storage_cost

    annual_capture = capacity_t_co2 * capture_rate * operating_hours
    energy_penalty_pct = additional_fuel_needed / baseline_fuel

Typical ranges:
    - Power plant CCS:     $40-80/tCO2
    - Industrial CCS:      $50-100/tCO2
    - DAC (current):       $300-1000/tCO2
    - DAC (2050 target):   $100-150/tCO2

**Enhanced Oil Recovery (EOR) Economics**:
    CO2-EOR uses injected CO2 to recover stranded oil.
    Net CO2 cost = gross_capture_cost - eor_revenue
    EOR revenue = oil_recovered_bbl * oil_price * eor_ratio
    CO2 utilization: 0.2-0.4 tCO2 permanently stored per bbl oil recovered

    CO2_EOR is carbon-negative when oil_price is high; net emitter when low.

**Direct Air Capture (DAC) Scaling**:
Wright's Law cost reduction with cumulative capacity:
    C(X) = C_0 * (X / X_0)^(-alpha)
    alpha = log(1 - LR) / log(2), LR ~ 15-20% for DAC

Policy carbon price required to be viable:
    breakeven_carbon_price = LCOC_DAC - any_subsidies_per_tco2

**Carbon Utilization Market**:
    CO2 -> concrete curing, e-fuels (e-methanol, e-SAF), enhanced agriculture
    Market size = sum over pathways of (capacity * $/tCO2_revenue)

Score: high CCS cost per ton with large abatement gap -> STRESS.
Competitive CCS with EOR revenue and DAC on learning curve -> STABLE.

Sources: IEA, GCCSI (Global CCS Institute), Carbon180, DOE, BloombergNEF
"""

import numpy as np

from app.layers.base import LayerBase


def _capital_recovery_factor(r: float, n: int) -> float:
    if r <= 0:
        return 1.0 / n
    return r * (1 + r) ** n / ((1 + r) ** n - 1)


class CarbonCapture(LayerBase):
    layer_id = "l16"
    name = "Carbon Capture"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")

        series_map = {
            "ccs_capex": f"CCS_CAPEX_PER_T_{country}",
            "ccs_opex": f"CCS_OPEX_PER_T_{country}",
            "ccs_capacity_mt": f"CCS_CAPACITY_MT_{country}",
            "ccs_captured_mt": f"CCS_CAPTURED_MT_{country}",
            "carbon_price": f"CARBON_PRICE_{country}",
            "oil_price": f"OIL_PRICE_{country}",
            "eor_co2_injection_mt": f"EOR_CO2_INJECTION_MT_{country}",
            "eor_oil_recovered_mb": f"EOR_OIL_RECOVERED_MB_{country}",
            "dac_capacity_mt": f"DAC_CAPACITY_MT_{country}",
            "dac_capex_per_t": f"DAC_CAPEX_PER_T_{country}",
            "co2_utilization_mt": f"CO2_UTILIZATION_MT_{country}",
            "industrial_co2_mt": f"INDUSTRIAL_CO2_MT_{country}",
            "ccs_target_mt": f"CCS_TARGET_MT_{country}",
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

        r = 0.08
        n = 25
        crf = _capital_recovery_factor(r, n)

        # --- CCS cost per tonne CO2 ---
        ccs_cost_per_t = None
        if "ccs_capex" in data:
            capex_vals = list(data["ccs_capex"].values())
            capex_per_t = float(capex_vals[-1]) if capex_vals else 200.0  # $/tCO2 capacity

            opex = 15.0  # $/tCO2
            if "ccs_opex" in data:
                opex_vals = list(data["ccs_opex"].values())
                opex = float(opex_vals[-1]) if opex_vals else opex

            transport_storage = 10.0  # $/tCO2 (pipeline + injection)
            # Annualized capex
            capex_annualized = capex_per_t * crf
            ccs_cost_per_t = capex_annualized + opex + transport_storage
            results["ccs_cost_per_tco2"] = round(float(ccs_cost_per_t), 1)
            results["ccs_cost_breakdown"] = {
                "capex_annualized_per_t": round(float(capex_annualized), 1),
                "opex_per_t": round(float(opex), 1),
                "transport_storage_per_t": round(float(transport_storage), 1),
            }

        # Carbon price comparison
        if "carbon_price" in data:
            cp_vals = list(data["carbon_price"].values())
            carbon_price = float(cp_vals[-1]) if cp_vals else 0
            results["carbon_price"] = round(carbon_price, 1)
            if ccs_cost_per_t is not None:
                results["ccs_viable_at_carbon_price"] = carbon_price >= ccs_cost_per_t

        # --- CCS deployment gap ---
        if "ccs_captured_mt" in data and "ccs_target_mt" in data:
            captured_vals = list(data["ccs_captured_mt"].values())
            target_vals = list(data["ccs_target_mt"].values())
            captured = float(captured_vals[-1]) if captured_vals else 0
            target = float(target_vals[-1]) if target_vals else 0
            gap = max(target - captured, 0)
            results["deployment_gap"] = {
                "captured_mt": round(captured, 2),
                "target_mt": round(target, 2),
                "gap_mt": round(float(gap), 2),
                "on_track": gap <= 0,
            }

        # --- EOR economics ---
        if "oil_price" in data:
            op_vals = list(data["oil_price"].values())
            oil_price = float(op_vals[-1]) if op_vals else 70.0  # $/bbl

            # CO2-EOR: 0.3 tCO2 per bbl, revenue from oil at $70 minus lifting cost $15
            eor_co2_per_bbl = 0.30  # tCO2 stored per bbl
            lifting_cost = 15.0  # $/bbl
            eor_net_revenue_per_tco2 = (oil_price - lifting_cost) * eor_co2_per_bbl

            eor_co2_mt = 0.0
            if "eor_co2_injection_mt" in data:
                ei_vals = list(data["eor_co2_injection_mt"].values())
                eor_co2_mt = float(ei_vals[-1]) if ei_vals else 0

            eor_oil_mb = 0.0
            if "eor_oil_recovered_mb" in data:
                eo_vals = list(data["eor_oil_recovered_mb"].values())
                eor_oil_mb = float(eo_vals[-1]) if eo_vals else 0

            eor_revenue_bn = eor_oil_mb * 1e6 * (oil_price - lifting_cost) / 1e9

            ccs_cost_for_eor = ccs_cost_per_t if ccs_cost_per_t else 60.0
            net_cost_with_eor = max(ccs_cost_for_eor - eor_net_revenue_per_tco2, 0)

            results["eor_economics"] = {
                "oil_price_per_bbl": round(oil_price, 1),
                "eor_revenue_per_tco2": round(float(eor_net_revenue_per_tco2), 1),
                "net_ccs_cost_with_eor": round(float(net_cost_with_eor), 1),
                "eor_co2_injected_mt": round(eor_co2_mt, 3),
                "eor_oil_recovered_mb": round(eor_oil_mb, 2),
                "eor_revenue_bn_usd": round(float(eor_revenue_bn), 2),
                "carbon_negative": eor_net_revenue_per_tco2 > (ccs_cost_for_eor * 0.8),
            }

        # --- DAC scaling ---
        dac_cost = None
        if "dac_capex_per_t" in data:
            dac_vals = list(data["dac_capex_per_t"].values())
            dac_cost = float(dac_vals[-1]) if dac_vals else 500.0  # $/tCO2

            dac_capacity_mt = 0.0
            if "dac_capacity_mt" in data:
                dc_vals = list(data["dac_capacity_mt"].values())
                dac_capacity_mt = float(dc_vals[-1]) if dc_vals else 0

            # Wright's Law projection to 1 Gt/yr (1000 Mt)
            lr_dac = 0.15
            alpha_dac = -np.log2(1 - lr_dac)
            target_mt = 1000.0  # 1 GtCO2/yr
            current_cap = max(dac_capacity_mt, 0.001)  # avoid div by 0
            forecast_cost = dac_cost * (target_mt / current_cap) ** (-alpha_dac)

            cp = results.get("carbon_price", 0)
            breakeven_price = dac_cost  # Current breakeven = current cost

            results["dac_scaling"] = {
                "current_cost_per_tco2": round(float(dac_cost), 0),
                "current_capacity_mt": round(float(dac_capacity_mt), 4),
                "forecast_cost_at_1gt_per_yr": round(float(forecast_cost), 0),
                "learning_rate": lr_dac,
                "breakeven_carbon_price": round(float(breakeven_price), 0),
                "currently_viable": float(cp) >= float(dac_cost),
            }

        # --- Carbon utilization market ---
        if "co2_utilization_mt" in data:
            util_vals = list(data["co2_utilization_mt"].values())
            util_mt = float(util_vals[-1]) if util_vals else 0
            # Average revenue from utilization ~$50-80/tCO2 (chemicals, concrete, fuels)
            avg_revenue = 65.0  # $/tCO2
            market_value_mn = util_mt * avg_revenue
            results["carbon_utilization"] = {
                "utilization_mt": round(util_mt, 3),
                "estimated_market_value_mn_usd": round(float(market_value_mn), 1),
            }

        # --- Score ---
        score = 30.0

        # CCS cost vs carbon price gap
        if ccs_cost_per_t is not None:
            cp = results.get("carbon_price", 0) or 0
            if cp < ccs_cost_per_t:
                gap_frac = (ccs_cost_per_t - cp) / ccs_cost_per_t
                score += min(gap_frac * 40, 30)

        # Deployment gap
        dep_info = results.get("deployment_gap", {})
        if dep_info:
            target = dep_info.get("target_mt", 0) or 1
            gap = dep_info.get("gap_mt", 0) or 0
            score += min((gap / target) * 25, 25)

        # DAC viability
        dac_info = results.get("dac_scaling", {})
        if dac_info and not dac_info.get("currently_viable"):
            score += 10
        elif dac_info and dac_info.get("currently_viable"):
            score -= 5

        score = float(np.clip(score, 0, 100))
        return {"score": round(score, 1), "results": results}
