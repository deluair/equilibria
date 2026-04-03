"""Electricity market analysis: merit order, LMP, capacity markets, intermittency costs.

Methodology
-----------
**Merit order curve**: generators dispatched in ascending marginal cost order.
The intersection of aggregate supply (merit order) with demand determines the
market clearing price. Modeled as a piecewise linear supply curve constructed
from generation capacity and variable cost data by technology.

    Technologies ordered by typical marginal cost ($/MWh):
    Nuclear (~5) < Wind/Solar (~0-3) < Coal (~25-40) < Gas CCGT (~30-50)
    < Gas peaker (~60-100) < Oil (~80-150)

    Clearing price = marginal cost of the last (most expensive) dispatched unit.

**Locational Marginal Pricing (LMP)**:
    LMP = energy_component + congestion_component + loss_component
    Estimated from nodal price data. Congestion rent = sum of price differences
    across constrained transmission lines. High congestion indicates grid
    investment needs.

**Capacity market design**: computes de-rated capacity margin (available
capacity minus peak demand, adjusted for forced outage rates). Adequate margin
typically >15%. Price estimated via demand curve intersection.

**Renewable intermittency costs**:
    - Balancing cost: extra reserves needed due to forecast error
    - Profile cost: value of generation relative to flat baseload
    - Grid cost: transmission reinforcement for remote renewables
    System LCOE = plant LCOE + integration cost

    Intermittency measured via coefficient of variation of renewable output
    and correlation with demand.

Score reflects electricity market stress: thin capacity margins, high
congestion, and large intermittency costs raise the score.

Sources: EIA, FERC, ISO market data
"""

import numpy as np

from app.layers.base import LayerBase


class ElectricityMarket(LayerBase):
    layer_id = "l16"
    name = "Electricity Market"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")

        series_map = {
            "demand": f"ELECTRICITY_DEMAND_{country}",
            "peak_demand": f"PEAK_DEMAND_{country}",
            "installed_capacity": f"INSTALLED_CAPACITY_{country}",
            "capacity_nuclear": f"CAPACITY_NUCLEAR_{country}",
            "capacity_coal": f"CAPACITY_COAL_{country}",
            "capacity_gas": f"CAPACITY_GAS_{country}",
            "capacity_wind": f"CAPACITY_WIND_{country}",
            "capacity_solar": f"CAPACITY_SOLAR_{country}",
            "capacity_hydro": f"CAPACITY_HYDRO_{country}",
            "wholesale_price": f"WHOLESALE_ELEC_PRICE_{country}",
            "congestion_cost": f"CONGESTION_COST_{country}",
            "renewable_output": f"RENEWABLE_OUTPUT_{country}",
            "forced_outage_rate": f"FORCED_OUTAGE_RATE_{country}",
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

        # --- Merit order curve ---
        tech_costs = {
            "wind": 0, "solar": 0, "nuclear": 5, "hydro": 3,
            "coal": 35, "gas": 45,
        }
        tech_capacities = {}
        for tech in tech_costs:
            cap_key = f"capacity_{tech}"
            if cap_key in data:
                vals = list(data[cap_key].values())
                tech_capacities[tech] = float(vals[-1]) if vals else 0

        if tech_capacities and "demand" in data:
            demand_vals = list(data["demand"].values())
            avg_demand = float(np.mean(demand_vals[-12:])) if len(demand_vals) >= 12 else float(
                np.mean(demand_vals))

            # Build merit order: sort by cost, accumulate capacity
            ordered_techs = sorted(tech_capacities.keys(), key=lambda t: tech_costs[t])
            cumulative_cap = 0
            merit_order = []
            clearing_price = 0
            marginal_tech = None

            for tech in ordered_techs:
                cap = tech_capacities[tech]
                cost = tech_costs[tech]
                merit_order.append({
                    "technology": tech,
                    "capacity_mw": round(cap, 0),
                    "marginal_cost": cost,
                    "cumulative_mw": round(cumulative_cap + cap, 0),
                })
                if cumulative_cap < avg_demand <= cumulative_cap + cap:
                    clearing_price = cost
                    marginal_tech = tech
                cumulative_cap += cap

            # If demand exceeds all capacity
            if cumulative_cap < avg_demand:
                clearing_price = 150  # scarcity pricing
                marginal_tech = "scarcity"

            results["merit_order"] = {
                "technologies": merit_order,
                "clearing_price": round(clearing_price, 1),
                "marginal_technology": marginal_tech,
                "total_capacity_mw": round(cumulative_cap, 0),
                "average_demand_mw": round(avg_demand, 0),
                "reserve_margin_pct": round((cumulative_cap - avg_demand) / avg_demand * 100, 1)
                if avg_demand > 0 else 0,
            }

        # --- Locational Marginal Pricing ---
        if "wholesale_price" in data and "congestion_cost" in data:
            common_lmp = sorted(set(data["wholesale_price"]) & set(data["congestion_cost"]))
            if common_lmp:
                prices = np.array([data["wholesale_price"][d] for d in common_lmp])
                congestion = np.array([data["congestion_cost"][d] for d in common_lmp])

                congestion_share = congestion / prices
                congestion_share = congestion_share[np.isfinite(congestion_share)]

                results["lmp_analysis"] = {
                    "avg_price": round(float(np.mean(prices)), 2),
                    "price_std": round(float(np.std(prices)), 2),
                    "avg_congestion": round(float(np.mean(congestion)), 2),
                    "congestion_share_of_price": round(float(np.mean(congestion_share)), 3)
                    if len(congestion_share) > 0 else None,
                    "price_spikes_above_100": int(np.sum(prices > 100)),
                    "n_obs": len(common_lmp),
                    "high_congestion": float(np.mean(congestion_share)) > 0.15
                    if len(congestion_share) > 0 else False,
                }

        # --- Capacity market (de-rated margin) ---
        if "installed_capacity" in data and "peak_demand" in data:
            common_cap = sorted(set(data["installed_capacity"]) & set(data["peak_demand"]))
            if common_cap:
                latest_d = common_cap[-1]
                total_cap = data["installed_capacity"][latest_d]
                peak = data["peak_demand"][latest_d]

                # Apply forced outage rate
                for_val = 0.07  # default 7% forced outage
                if "forced_outage_rate" in data:
                    for_vals = list(data["forced_outage_rate"].values())
                    for_val = float(for_vals[-1]) / 100 if for_vals else 0.07

                derated_cap = total_cap * (1 - for_val)
                derated_margin = (derated_cap - peak) / peak * 100 if peak > 0 else 0

                # Capacity price estimate: linear demand curve
                # Price = max_price * (1 - margin / target_margin)
                target_margin = 15.0  # typical adequacy standard
                max_price = 300  # $/MW-day cap
                if derated_margin < target_margin:
                    cap_price = max_price * (1 - derated_margin / target_margin)
                    cap_price = max(cap_price, 0)
                else:
                    cap_price = 0

                results["capacity_market"] = {
                    "installed_capacity_mw": round(total_cap, 0),
                    "peak_demand_mw": round(peak, 0),
                    "forced_outage_rate": round(for_val, 3),
                    "derated_capacity_mw": round(derated_cap, 0),
                    "derated_margin_pct": round(float(derated_margin), 1),
                    "adequate": float(derated_margin) >= target_margin,
                    "estimated_capacity_price": round(float(cap_price), 1),
                    "date": latest_d,
                }

        # --- Renewable intermittency costs ---
        if "renewable_output" in data and "demand" in data:
            common_re = sorted(set(data["renewable_output"]) & set(data["demand"]))
            if len(common_re) >= 12:
                re_vals = np.array([data["renewable_output"][d] for d in common_re])
                dm_vals = np.array([data["demand"][d] for d in common_re])

                # Coefficient of variation
                cv = float(np.std(re_vals) / np.mean(re_vals)) if np.mean(re_vals) > 0 else 0

                # Correlation with demand (higher = better, lower intermittency cost)
                corr = float(np.corrcoef(re_vals, dm_vals)[0, 1]) if len(re_vals) > 2 else 0

                # Capacity factor
                cap_factor = float(np.mean(re_vals) / np.max(re_vals)) if np.max(re_vals) > 0 else 0

                # Integration cost estimate (simplified)
                # Balancing cost ~ proportional to CV
                balancing_cost = cv * 5  # $/MWh
                # Profile cost ~ inversely proportional to demand correlation
                profile_cost = max((1 - corr) * 8, 0)
                # Grid cost ~ proportional to penetration
                penetration = float(np.mean(re_vals) / np.mean(dm_vals)) if np.mean(dm_vals) > 0 else 0
                grid_cost = penetration * 3  # $/MWh

                total_integration = balancing_cost + profile_cost + grid_cost

                results["intermittency"] = {
                    "cv_output": round(cv, 3),
                    "demand_correlation": round(corr, 3),
                    "capacity_factor": round(cap_factor, 3),
                    "penetration_share": round(penetration, 3),
                    "balancing_cost_per_mwh": round(float(balancing_cost), 2),
                    "profile_cost_per_mwh": round(float(profile_cost), 2),
                    "grid_cost_per_mwh": round(float(grid_cost), 2),
                    "total_integration_cost_per_mwh": round(float(total_integration), 2),
                    "n_obs": len(common_re),
                }

        # --- Score ---
        score = 15.0

        # Capacity margin
        cap_info = results.get("capacity_market", {})
        if cap_info:
            margin = cap_info.get("derated_margin_pct", 15)
            if margin < 5:
                score += 30
            elif margin < 10:
                score += 20
            elif margin < 15:
                score += 10

        # Congestion
        lmp_info = results.get("lmp_analysis", {})
        if lmp_info:
            if lmp_info.get("high_congestion"):
                score += 15
            spikes = lmp_info.get("price_spikes_above_100", 0)
            score += min(spikes * 2, 10)

        # Intermittency costs
        inter_info = results.get("intermittency", {})
        if inter_info:
            total_int = inter_info.get("total_integration_cost_per_mwh", 0)
            score += min(total_int, 15)

        # Merit order: scarcity
        mo_info = results.get("merit_order", {})
        if mo_info:
            if mo_info.get("marginal_technology") == "scarcity":
                score += 20

        score = float(np.clip(score, 0, 100))

        return {"score": round(score, 1), "results": results}
