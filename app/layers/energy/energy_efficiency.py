"""Energy efficiency: rebound effect, LMDI decomposition, building savings, industrial audit.

Methodology
-----------
**Rebound effect (Jevons paradox)**:
Efficiency improvements reduce effective price of energy services, inducing
higher consumption that partially offsets savings. Estimated via elasticity:

    Rebound = 1 - (actual_savings / engineering_savings)

Direct rebound measured from energy demand elasticity w.r.t. efficiency-adjusted
price. Uses OLS on log-log specification:

    ln(E_t) = alpha + beta * ln(P_eff_t) + gamma * ln(Y_t) + eps_t

where P_eff = energy price / efficiency index, E = energy consumption, Y = income.
Direct rebound = -beta. Backfire if rebound > 100% (beta < -1).

**LMDI (Log Mean Divisia Index) decomposition** (Ang 2004):
Decomposes change in total energy consumption into:
    delta_E = activity_effect + structure_effect + intensity_effect

    activity_effect   = sum_i L(E_i^1, E_i^0) * ln(Y^1 / Y^0)
    structure_effect  = sum_i L(E_i^1, E_i^0) * ln(S_i^1 / S_i^0)
    intensity_effect  = sum_i L(E_i^1, E_i^0) * ln(I_i^1 / I_i^0)

where L(a,b) = (a-b)/ln(a/b) is the log-mean, Y = total output,
S_i = sector i share of output, I_i = energy intensity of sector i.

Perfect decomposition: no residual (unique LMDI property).

**Building energy savings**: engineering bottom-up estimate of savings from
envelope, HVAC, and lighting improvements. Net savings = gross - rebound.

**Industrial energy audit**: energy intensity benchmarking by sector against
best-available-technology (BAT). Efficiency gap = actual / BAT intensity.

Score reflects energy inefficiency: large rebound, rising intensity, and
wide BAT gaps raise the score.

Sources: EIA, IEA, World Bank WDI
"""

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


def _log_mean(a: float, b: float) -> float:
    """Log-mean function L(a, b) = (a - b) / ln(a/b). Returns a if a == b."""
    if a <= 0 or b <= 0:
        return 0.0
    if abs(a - b) < 1e-12:
        return a
    return (a - b) / np.log(a / b)


class EnergyEfficiency(LayerBase):
    layer_id = "l16"
    name = "Energy Efficiency"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        n_sectors = kwargs.get("n_sectors", 5)

        series_map = {
            "energy_consumption": f"ENERGY_CONSUMPTION_{country}",
            "energy_price": f"ENERGY_PRICE_{country}",
            "efficiency_index": f"ENERGY_EFFICIENCY_INDEX_{country}",
            "gdp": f"GDP_{country}",
            "population": f"POPULATION_{country}",
            "total_output": f"TOTAL_OUTPUT_{country}",
        }
        # Sector-level data
        sector_names = ["industry", "transport", "residential", "commercial", "agriculture"]
        for sec in sector_names[:n_sectors]:
            series_map[f"energy_{sec}"] = f"ENERGY_{sec.upper()}_{country}"
            series_map[f"output_{sec}"] = f"OUTPUT_{sec.upper()}_{country}"
            series_map[f"bat_{sec}"] = f"BAT_INTENSITY_{sec.upper()}_{country}"

        # Building-specific
        series_map["building_envelope_savings"] = f"BUILDING_ENVELOPE_SAVINGS_{country}"
        series_map["hvac_savings"] = f"HVAC_SAVINGS_{country}"
        series_map["lighting_savings"] = f"LIGHTING_SAVINGS_{country}"

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

        # --- Rebound effect estimation ---
        if all(k in data for k in ["energy_consumption", "energy_price", "efficiency_index", "gdp"]):
            common = sorted(set.intersection(*[set(data[k]) for k in
                            ["energy_consumption", "energy_price", "efficiency_index", "gdp"]]))

            if len(common) >= 15:
                E = np.array([data["energy_consumption"][d] for d in common])
                P = np.array([data["energy_price"][d] for d in common])
                eff = np.array([data["efficiency_index"][d] for d in common])
                Y = np.array([data["gdp"][d] for d in common])

                # Effective price: price adjusted for efficiency
                P_eff = P / eff

                # Log-log regression
                ln_E = np.log(E)
                ln_Peff = np.log(P_eff)
                ln_Y = np.log(Y)

                X_reb = np.column_stack([np.ones(len(common)), ln_Peff, ln_Y])
                beta = np.linalg.lstsq(X_reb, ln_E, rcond=None)[0]
                resid = ln_E - X_reb @ beta
                n_obs = len(common)

                # HC1 standard errors
                sse = float(np.sum(resid ** 2))
                sst = float(np.sum((ln_E - np.mean(ln_E)) ** 2))
                r_sq = 1 - sse / sst if sst > 0 else 0
                bread = np.linalg.inv(X_reb.T @ X_reb)
                meat = X_reb.T @ np.diag(resid ** 2) @ X_reb
                vcov = (n_obs / (n_obs - 3)) * bread @ meat @ bread
                se = np.sqrt(np.diag(vcov))

                price_elasticity = float(beta[1])
                direct_rebound = -price_elasticity
                income_elasticity = float(beta[2])

                t_stat_price = price_elasticity / se[1] if se[1] > 0 else 0
                p_value_price = 2 * (1 - sp_stats.t.cdf(abs(t_stat_price), n_obs - 3))

                results["rebound_effect"] = {
                    "price_elasticity": round(price_elasticity, 4),
                    "price_se": round(float(se[1]), 4),
                    "price_pvalue": round(float(p_value_price), 4),
                    "income_elasticity": round(income_elasticity, 4),
                    "direct_rebound_pct": round(direct_rebound * 100, 1),
                    "backfire": direct_rebound > 1.0,
                    "r_squared": round(r_sq, 4),
                    "n_obs": n_obs,
                    "interpretation": (
                        "backfire" if direct_rebound > 1.0
                        else "large rebound" if direct_rebound > 0.5
                        else "moderate rebound" if direct_rebound > 0.2
                        else "small rebound" if direct_rebound > 0
                        else "no rebound or super-conservation"
                    ),
                }

        # --- LMDI decomposition ---
        sector_data = {}
        for sec in sector_names[:n_sectors]:
            e_key = f"energy_{sec}"
            o_key = f"output_{sec}"
            if e_key in data and o_key in data:
                sector_data[sec] = {"energy": data[e_key], "output": data[o_key]}

        if sector_data and "total_output" in data:
            # Find common dates across all sectors and total output
            all_date_sets = [set(sd["energy"].keys()) & set(sd["output"].keys())
                             for sd in sector_data.values()]
            all_date_sets.append(set(data["total_output"].keys()))
            common_lmdi = sorted(set.intersection(*all_date_sets))

            if len(common_lmdi) >= 2:
                # Decompose between first and last period
                d0, d1 = common_lmdi[0], common_lmdi[-1]
                Y0_total = data["total_output"][d0]
                Y1_total = data["total_output"][d1]

                activity_eff = 0.0
                structure_eff = 0.0
                intensity_eff = 0.0
                sector_details = []

                for sec, sd in sector_data.items():
                    E0 = sd["energy"][d0]
                    E1 = sd["energy"][d1]
                    O0 = sd["output"][d0]
                    O1 = sd["output"][d1]

                    if E0 <= 0 or E1 <= 0 or O0 <= 0 or O1 <= 0 or Y0_total <= 0 or Y1_total <= 0:
                        continue

                    S0 = O0 / Y0_total
                    S1 = O1 / Y1_total
                    I0 = E0 / O0
                    I1 = E1 / O1

                    L = _log_mean(E1, E0)

                    act = L * np.log(Y1_total / Y0_total)
                    struc = L * np.log(S1 / S0) if S0 > 0 and S1 > 0 else 0
                    inten = L * np.log(I1 / I0) if I0 > 0 and I1 > 0 else 0

                    activity_eff += act
                    structure_eff += struc
                    intensity_eff += inten

                    sector_details.append({
                        "sector": sec,
                        "activity": round(float(act), 2),
                        "structure": round(float(struc), 2),
                        "intensity": round(float(inten), 2),
                        "intensity_change_pct": round((I1 / I0 - 1) * 100, 1) if I0 > 0 else None,
                    })

                total_change = activity_eff + structure_eff + intensity_eff

                results["lmdi_decomposition"] = {
                    "period": f"{d0} to {d1}",
                    "activity_effect": round(float(activity_eff), 2),
                    "structure_effect": round(float(structure_eff), 2),
                    "intensity_effect": round(float(intensity_eff), 2),
                    "total_change": round(float(total_change), 2),
                    "intensity_improving": float(intensity_eff) < 0,
                    "sectors": sector_details,
                }

        # --- Building energy savings ---
        building_components = {}
        for comp in ["building_envelope_savings", "hvac_savings", "lighting_savings"]:
            if comp in data:
                vals = list(data[comp].values())
                building_components[comp.replace("_savings", "")] = float(vals[-1]) if vals else 0

        if building_components:
            gross_savings = sum(building_components.values())
            rebound_pct = results.get("rebound_effect", {}).get("direct_rebound_pct", 20) / 100
            net_savings = gross_savings * (1 - rebound_pct)

            results["building_savings"] = {
                "components": {k: round(v, 1) for k, v in building_components.items()},
                "gross_savings_pct": round(gross_savings, 1),
                "rebound_adjustment_pct": round(rebound_pct * 100, 1),
                "net_savings_pct": round(float(net_savings), 1),
            }

        # --- Industrial energy audit (BAT benchmarking) ---
        audit_results = []
        for sec in sector_names[:n_sectors]:
            e_key = f"energy_{sec}"
            o_key = f"output_{sec}"
            bat_key = f"bat_{sec}"
            if all(k in data for k in [e_key, o_key, bat_key]):
                e_vals = list(data[e_key].values())
                o_vals = list(data[o_key].values())
                bat_vals = list(data[bat_key].values())

                if e_vals and o_vals and bat_vals:
                    actual_intensity = e_vals[-1] / o_vals[-1] if o_vals[-1] > 0 else 0
                    bat_intensity = bat_vals[-1]
                    gap = actual_intensity / bat_intensity if bat_intensity > 0 else 1

                    audit_results.append({
                        "sector": sec,
                        "actual_intensity": round(float(actual_intensity), 4),
                        "bat_intensity": round(float(bat_intensity), 4),
                        "efficiency_gap": round(float(gap), 2),
                        "savings_potential_pct": round(max((1 - 1 / gap) * 100, 0), 1) if gap > 1 else 0,
                    })

        if audit_results:
            avg_gap = float(np.mean([a["efficiency_gap"] for a in audit_results]))
            results["industrial_audit"] = {
                "sectors": audit_results,
                "average_efficiency_gap": round(avg_gap, 2),
                "worst_sector": max(audit_results, key=lambda x: x["efficiency_gap"])["sector"]
                if audit_results else None,
            }

        # --- Energy intensity trend ---
        if "energy_consumption" in data and "gdp" in data:
            common_ei = sorted(set(data["energy_consumption"]) & set(data["gdp"]))
            if len(common_ei) >= 5:
                ei_vals = np.array([
                    data["energy_consumption"][d] / data["gdp"][d]
                    for d in common_ei
                    if data["gdp"][d] > 0
                ])
                if len(ei_vals) >= 5:
                    t_arr = np.arange(len(ei_vals), dtype=float)
                    slope, intercept, r, p, se = sp_stats.linregress(t_arr, ei_vals)
                    results["energy_intensity_trend"] = {
                        "latest": round(float(ei_vals[-1]), 4),
                        "slope": round(float(slope), 6),
                        "r_squared": round(float(r ** 2), 4),
                        "p_value": round(float(p), 4),
                        "improving": float(slope) < 0 and float(p) < 0.10,
                    }

        # --- Score ---
        score = 20.0

        # Rebound effect
        reb_info = results.get("rebound_effect", {})
        if reb_info:
            reb_pct = reb_info.get("direct_rebound_pct", 0)
            if reb_info.get("backfire"):
                score += 25
            elif reb_pct > 50:
                score += 15
            elif reb_pct > 30:
                score += 8

        # Energy intensity trend
        ei_info = results.get("energy_intensity_trend", {})
        if ei_info:
            if not ei_info.get("improving"):
                score += 15

        # LMDI intensity effect
        lmdi_info = results.get("lmdi_decomposition", {})
        if lmdi_info:
            if not lmdi_info.get("intensity_improving"):
                score += 10

        # Industrial audit gap
        audit_info = results.get("industrial_audit", {})
        if audit_info:
            avg_gap = audit_info.get("average_efficiency_gap", 1)
            score += min(max((avg_gap - 1) * 20, 0), 15)

        score = float(np.clip(score, 0, 100))

        return {"score": round(score, 1), "results": results}
