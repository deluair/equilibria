"""Nuclear economics: LCOE vs alternatives, decommissioning liability, SMR economics, waste costs.

Methodology
-----------
**Levelized Cost of Electricity (LCOE)**:
    LCOE = (overnight_capex * CRF + fixed_opex + decommissioning_annuity) / (CF * 8760)
           + variable_opex + fuel_cost

Capital recovery factor:
    CRF = r * (1+r)^n / [(1+r)^n - 1]

Nuclear-specific adders:
    decommissioning_annuity = (decom_cost_bn * 1e9) * CRF_decom (over 40-yr fund period)
    waste_cost_per_MWh      = interim + final_repository_share (cents/kWh)

**Decommissioning Liability**:
    Net liability = estimated_future_cost - accumulated_fund
    Funding ratio = fund_balance / PV(future_costs)
    Underfunding indicates off-balance-sheet liability risk.

**Small Modular Reactor (SMR) Economics**:
    LCOE_SMR = (FOAK_capex * CRF + opex) / (CF * 8760)
    Factory economies: NOAK cost = FOAK * (series_factor)^(-alpha_smr)
    alpha_smr from Wright's Law; typical LR for SMR 10-15%.
    Break-even vs. large nuclear at nth-of-a-kind (NOAK) production volume.

**Waste Storage Costs**:
    Interim storage: dry cask ~$80-120/kg HM/yr
    Deep geological repository: ~$150-400/kg HM one-time
    Total waste liability per MWh from historical fleet production.

Score: high LCOE relative to alternatives + large underfunded decommissioning liability
       + unresolved waste -> STRESS. Competitive LCOE + fully funded + waste solution -> STABLE.

Sources: IEA, WNA, NEA/IAEA, EIA, national regulators
"""

import numpy as np

from app.layers.base import LayerBase


def _capital_recovery_factor(r: float, n: int) -> float:
    if r <= 0:
        return 1.0 / n
    return r * (1 + r) ** n / ((1 + r) ** n - 1)


class NuclearEconomics(LayerBase):
    layer_id = "l16"
    name = "Nuclear Economics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")

        series_map = {
            "nuclear_capex": f"NUCLEAR_OVERNIGHT_CAPEX_{country}",
            "nuclear_opex_fixed": f"NUCLEAR_OPEX_FIXED_{country}",
            "nuclear_opex_variable": f"NUCLEAR_OPEX_VARIABLE_{country}",
            "nuclear_fuel_cost": f"NUCLEAR_FUEL_COST_{country}",
            "nuclear_capacity_factor": f"NUCLEAR_CAPACITY_FACTOR_{country}",
            "nuclear_capacity_gw": f"NUCLEAR_CAPACITY_GW_{country}",
            "decom_fund_balance": f"NUCLEAR_DECOM_FUND_{country}",
            "decom_estimated_cost": f"NUCLEAR_DECOM_COST_{country}",
            "waste_volume_thm": f"NUCLEAR_WASTE_VOLUME_THM_{country}",
            "smr_capex": f"SMR_OVERNIGHT_CAPEX_{country}",
            "solar_lcoe": f"SOLAR_LCOE_{country}",
            "wind_lcoe": f"WIND_LCOE_{country}",
            "gas_lcoe": f"GAS_LCOE_{country}",
            "nuclear_generation_twh": f"NUCLEAR_GENERATION_TWH_{country}",
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

        r = 0.07
        n_life = 60   # years (with life extension)
        crf = _capital_recovery_factor(r, n_life)

        # --- Nuclear LCOE ---
        nuclear_lcoe = None
        if "nuclear_capex" in data:
            capex_vals = list(data["nuclear_capex"].values())
            capex = float(capex_vals[-1]) if capex_vals else 6500.0  # $/kW

            cf = 0.90
            if "nuclear_capacity_factor" in data:
                cf_vals = list(data["nuclear_capacity_factor"].values())
                cf = float(cf_vals[-1]) / 100 if cf_vals else cf

            opex_fixed = 130.0  # $/kW-yr
            if "nuclear_opex_fixed" in data:
                ov = list(data["nuclear_opex_fixed"].values())
                opex_fixed = float(ov[-1]) if ov else opex_fixed

            opex_var = 3.0  # $/MWh
            if "nuclear_opex_variable" in data:
                ov2 = list(data["nuclear_opex_variable"].values())
                opex_var = float(ov2[-1]) if ov2 else opex_var

            fuel = 7.0  # $/MWh
            if "nuclear_fuel_cost" in data:
                fv = list(data["nuclear_fuel_cost"].values())
                fuel = float(fv[-1]) if fv else fuel

            # Decommissioning annuity over operating life
            decom_cost_per_kw = 1200.0  # $/kW (US avg ~$1,000-1,500/kW)
            if "decom_estimated_cost" in data and "nuclear_capacity_gw" in data:
                decom_vals = list(data["decom_estimated_cost"].values())
                cap_gw_vals = list(data["nuclear_capacity_gw"].values())
                decom_total = float(decom_vals[-1]) if decom_vals else 0
                cap_gw = float(cap_gw_vals[-1]) if cap_gw_vals else 1
                if cap_gw > 0:
                    decom_cost_per_kw = decom_total * 1e9 / (cap_gw * 1e6)

            crf_decom = _capital_recovery_factor(0.03, n_life)  # Decom fund at 3%
            decom_annuity_per_kw = decom_cost_per_kw * crf_decom
            annual_gen_per_kw = cf * 8760  # MWh/kW/yr
            decom_per_mwh = decom_annuity_per_kw / annual_gen_per_kw if annual_gen_per_kw > 0 else 0

            # Waste cost: ~$1-3/MWh
            waste_per_mwh = 2.0

            capex_per_mwh = (capex * crf) / annual_gen_per_kw if annual_gen_per_kw > 0 else 9999
            opex_fixed_per_mwh = opex_fixed / annual_gen_per_kw if annual_gen_per_kw > 0 else 0
            nuclear_lcoe = capex_per_mwh + opex_fixed_per_mwh + opex_var + fuel + decom_per_mwh + waste_per_mwh

            results["nuclear_lcoe_per_mwh"] = round(float(nuclear_lcoe), 1)
            results["lcoe_breakdown"] = {
                "capex_component": round(float(capex_per_mwh), 1),
                "opex_fixed_component": round(float(opex_fixed_per_mwh), 1),
                "opex_variable_component": round(float(opex_var), 1),
                "fuel_component": round(float(fuel), 1),
                "decommissioning_component": round(float(decom_per_mwh), 1),
                "waste_component": round(float(waste_per_mwh), 1),
                "capacity_factor": round(cf, 3),
            }

        # --- LCOE comparison ---
        comparisons = {}
        for alt in ["solar_lcoe", "wind_lcoe", "gas_lcoe"]:
            if alt in data:
                alt_vals = list(data[alt].values())
                alt_val = float(alt_vals[-1]) if alt_vals else None
                if alt_val is not None:
                    comparisons[alt.replace("_lcoe", "")] = round(alt_val, 1)
                    if nuclear_lcoe is not None:
                        comparisons[f"{alt.replace('_lcoe', '')}_premium"] = round(
                            float(nuclear_lcoe - alt_val), 1
                        )

        if comparisons:
            results["lcoe_comparison"] = comparisons

        # --- Decommissioning liability ---
        if "decom_fund_balance" in data and "decom_estimated_cost" in data:
            fund_vals = list(data["decom_fund_balance"].values())
            cost_vals = list(data["decom_estimated_cost"].values())
            fund_balance = float(fund_vals[-1]) if fund_vals else 0
            estimated_cost = float(cost_vals[-1]) if cost_vals else 0

            # PV of future decommissioning (discount at 3%)
            pv_decom = estimated_cost  # Assume nominal = PV (simplified)

            underfunding = max(pv_decom - fund_balance, 0)
            funding_ratio = fund_balance / pv_decom if pv_decom > 0 else 1.0

            results["decommissioning"] = {
                "fund_balance_bn": round(fund_balance, 2),
                "estimated_cost_bn": round(estimated_cost, 2),
                "underfunding_bn": round(float(underfunding), 2),
                "funding_ratio": round(float(funding_ratio), 3),
                "adequately_funded": funding_ratio >= 0.90,
            }

        # --- SMR economics ---
        if "smr_capex" in data:
            smr_vals = list(data["smr_capex"].values())
            smr_capex = float(smr_vals[-1]) if smr_vals else 8000.0  # FOAK $/kW
            cf_smr = 0.93  # Higher CF possible for SMR (load-following)
            opex_smr = 120.0  # $/kW-yr (potentially lower due to modular design)
            crf_smr = _capital_recovery_factor(0.08, 60)
            annual_gen_smr = cf_smr * 8760
            smr_lcoe = (smr_capex * crf_smr + opex_smr) / annual_gen_smr + fuel + waste_per_mwh if nuclear_lcoe else 0

            # NOAK projection: assume 10% learning rate, 10 units
            n_units = 10
            lr_smr = 0.12
            alpha_smr = -np.log2(1 - lr_smr)
            noak_capex = smr_capex * n_units ** (-alpha_smr)
            noak_lcoe = (noak_capex * crf_smr + opex_smr) / annual_gen_smr + (fuel if nuclear_lcoe else 7.0) + 2.0

            results["smr_economics"] = {
                "foak_capex_per_kw": round(float(smr_capex), 0),
                "foak_lcoe_per_mwh": round(float(smr_lcoe), 1),
                "noak_capex_per_kw_at_10_units": round(float(noak_capex), 0),
                "noak_lcoe_per_mwh": round(float(noak_lcoe), 1),
                "learning_rate_assumed": lr_smr,
                "vs_large_nuclear_premium": round(float(smr_lcoe - nuclear_lcoe), 1)
                if nuclear_lcoe else None,
            }

        # --- Waste storage costs ---
        if "waste_volume_thm" in data:
            waste_vals = list(data["waste_volume_thm"].values())
            waste_thm = float(waste_vals[-1]) if waste_vals else 0
            interim_cost_per_thm_yr = 100.0  # $/kg HM/yr
            repository_cost_per_thm = 300.0  # $/kg HM one-time
            interim_annual = waste_thm * interim_cost_per_thm_yr / 1e9  # $bn/yr
            repository_total = waste_thm * repository_cost_per_thm / 1e9  # $bn
            gen_twh = float(list(data["nuclear_generation_twh"].values())[-1]) if "nuclear_generation_twh" in data else 1
            waste_per_mwh_actual = (interim_annual * 1e9 / (gen_twh * 1e6)) if gen_twh > 0 else 0
            results["waste_storage"] = {
                "waste_volume_tonnes_hm": round(waste_thm, 0),
                "interim_storage_cost_bn_per_yr": round(float(interim_annual), 3),
                "repository_cost_bn_total": round(float(repository_total), 1),
                "waste_cost_per_mwh": round(float(waste_per_mwh_actual), 2),
            }

        # --- Score ---
        score = 25.0

        # LCOE premium vs alternatives
        if nuclear_lcoe is not None and comparisons:
            alt_values = [v for k, v in comparisons.items() if not k.endswith("_premium")]
            if alt_values:
                min_alt = min(alt_values)
                premium_pct = (nuclear_lcoe - min_alt) / min_alt * 100 if min_alt > 0 else 0
                score += min(max(premium_pct * 0.4, 0), 30)

        # Decommissioning underfunding
        decom_info = results.get("decommissioning", {})
        if decom_info:
            fr = decom_info.get("funding_ratio", 1.0)
            if fr < 0.9:
                score += (0.9 - fr) * 80  # 50% funded = +32 pts

        # Waste unresolved
        waste_info = results.get("waste_storage", {})
        repo_cost = waste_info.get("repository_cost_bn_total", 0) or 0
        if repo_cost > 50:
            score += min(repo_cost / 50, 15)

        score = float(np.clip(score, 0, 100))
        return {"score": round(score, 1), "results": results}
