"""Fossil fuel subsidy estimation: pre-tax, post-tax (IMF), welfare effects, political economy.

Methodology
-----------
**Pre-tax subsidies** (explicit):
    Direct fiscal cost when consumer prices are held below international
    reference prices. Measured as:
        pre_tax_subsidy = (reference_price - consumer_price) * consumption
    Reference price = border price + transport/distribution margin.

**Post-tax subsidies** (IMF methodology, Coady et al. 2019):
    Includes pre-tax subsidy plus failure to charge for externalities:
        post_tax_subsidy = pre_tax + local_pollution + CO2_damage
                          + congestion + accident_externalities
                          + forgone_consumption_tax

    CO2 damage: social cost of carbon ($50-80/tCO2 in 2020 USD) times
    emissions factor per fuel unit.
    Local air pollution: health damage from PM2.5, SOx, NOx.
    Congestion: vehicle-km based for transport fuels.

    IMF estimates global post-tax subsidies at ~$5.9 trillion/yr (6.8% GDP).

**Welfare effects of subsidy reform** (Araar & Verme 2012):
    Consumer surplus loss from price increase, offset by fiscal savings
    redistributed as transfers. Distributional impact depends on fuel budget
    shares by income quintile.

    Net welfare = fiscal_savings * redistribution_efficiency - consumer_loss
    Progressive if poor consume less fuel per capita (typically true for
    transport, less for kerosene/LPG).

**Political economy of reform** (Victor 2009):
    Reform difficulty index based on:
    - Subsidy size as % GDP (larger = harder)
    - Urban population share (urban consumers resist)
    - Past reform attempts and reversals
    - Institutional quality (capacity to compensate losers)

Score reflects subsidy distortion: large post-tax subsidies with regressive
distribution and reform resistance raise the score.

Sources: IMF, IEA, World Bank
"""

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class FossilSubsidy(LayerBase):
    layer_id = "l16"
    name = "Fossil Fuel Subsidy"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        scc = kwargs.get("social_cost_carbon", 65.0)  # $/tCO2

        series_map = {
            "oil_consumption": f"OIL_CONSUMPTION_{country}",
            "gas_consumption": f"GAS_CONSUMPTION_{country}",
            "coal_consumption": f"COAL_CONSUMPTION_{country}",
            "oil_ref_price": f"OIL_REFERENCE_PRICE_{country}",
            "oil_con_price": f"OIL_CONSUMER_PRICE_{country}",
            "gas_ref_price": f"GAS_REFERENCE_PRICE_{country}",
            "gas_con_price": f"GAS_CONSUMER_PRICE_{country}",
            "coal_ref_price": f"COAL_REFERENCE_PRICE_{country}",
            "coal_con_price": f"COAL_CONSUMER_PRICE_{country}",
            "co2_emissions": f"CO2_EMISSIONS_{country}",
            "gdp": f"GDP_{country}",
            "population": f"POPULATION_{country}",
            "urban_share": f"URBAN_SHARE_{country}",
            "institutional_quality": f"INSTITUTIONAL_QUALITY_{country}",
            "health_damage": f"AIR_POLLUTION_HEALTH_DAMAGE_{country}",
            "fuel_share_q1": f"FUEL_BUDGET_SHARE_Q1_{country}",
            "fuel_share_q5": f"FUEL_BUDGET_SHARE_Q5_{country}",
            "vat_rate": f"VAT_RATE_{country}",
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

        results = {"country": country, "social_cost_carbon": scc}

        # --- Pre-tax subsidies by fuel ---
        fuels = ["oil", "gas", "coal"]
        # Emission factors (tCO2 per TJ): IPCC defaults
        emission_factors = {"oil": 73.3, "gas": 56.1, "coal": 96.1}

        pre_tax_total = 0.0
        post_tax_total = 0.0
        fuel_results = {}

        for fuel in fuels:
            cons_key = f"{fuel}_consumption"
            ref_key = f"{fuel}_ref_price"
            con_key = f"{fuel}_con_price"

            if not all(k in data for k in [cons_key, ref_key, con_key]):
                continue

            common = sorted(
                set(data[cons_key]) & set(data[ref_key]) & set(data[con_key])
            )
            if not common:
                continue

            latest = common[-1]
            consumption = data[cons_key][latest]
            ref_price = data[ref_key][latest]
            con_price = data[con_key][latest]

            # Pre-tax subsidy
            price_gap = max(ref_price - con_price, 0)
            pre_tax = price_gap * consumption

            # CO2 externality cost
            ef = emission_factors[fuel]
            co2_cost = ef * consumption * scc / 1e6  # scale to same units

            # Local air pollution damage
            if "health_damage" in data:
                health_vals = list(data["health_damage"].values())
                health_cost = float(health_vals[-1]) * consumption / sum(
                    data.get(f"{f}_consumption", {}).get(latest, 1) for f in fuels
                ) if health_vals else 0
            else:
                health_cost = 0

            # Forgone consumption tax
            vat = 0.0
            if "vat_rate" in data:
                vat_vals = list(data["vat_rate"].values())
                vat_rate = float(vat_vals[-1]) / 100 if vat_vals else 0
                vat = con_price * consumption * vat_rate

            post_tax = pre_tax + co2_cost + health_cost + vat

            fuel_results[fuel] = {
                "consumption": round(consumption, 1),
                "reference_price": round(ref_price, 2),
                "consumer_price": round(con_price, 2),
                "price_gap": round(float(price_gap), 2),
                "pre_tax_subsidy": round(float(pre_tax), 1),
                "co2_externality_cost": round(float(co2_cost), 1),
                "health_damage_cost": round(float(health_cost), 1),
                "forgone_tax": round(float(vat), 1),
                "post_tax_subsidy": round(float(post_tax), 1),
            }

            pre_tax_total += pre_tax
            post_tax_total += post_tax

        if fuel_results:
            gdp_val = None
            if "gdp" in data:
                gdp_vals = list(data["gdp"].values())
                gdp_val = float(gdp_vals[-1]) if gdp_vals else None

            results["subsidies"] = {
                "by_fuel": fuel_results,
                "pre_tax_total": round(float(pre_tax_total), 1),
                "post_tax_total": round(float(post_tax_total), 1),
                "pre_tax_pct_gdp": round(pre_tax_total / gdp_val * 100, 2) if gdp_val else None,
                "post_tax_pct_gdp": round(post_tax_total / gdp_val * 100, 2) if gdp_val else None,
                "externality_share": round(
                    (post_tax_total - pre_tax_total) / post_tax_total * 100, 1
                ) if post_tax_total > 0 else 0,
            }

        # --- Welfare effects of reform ---
        if fuel_results and "fuel_share_q1" in data and "fuel_share_q5" in data:
            q1_vals = list(data["fuel_share_q1"].values())
            q5_vals = list(data["fuel_share_q5"].values())
            pop_vals = list(data["population"].values()) if "population" in data else []

            if q1_vals and q5_vals:
                q1_share = float(q1_vals[-1]) / 100  # budget share of poorest quintile
                q5_share = float(q5_vals[-1]) / 100

                # Price increase from removing pre-tax subsidy (average across fuels)
                avg_price_increase = 0
                n_fuels = 0
                for fuel, fr in fuel_results.items():
                    if fr["reference_price"] > 0 and fr["consumer_price"] > 0:
                        pct_inc = (fr["reference_price"] - fr["consumer_price"]) / fr["consumer_price"]
                        avg_price_increase += max(pct_inc, 0)
                        n_fuels += 1
                avg_price_increase = avg_price_increase / n_fuels if n_fuels > 0 else 0

                # Consumer loss by quintile
                q1_loss = q1_share * avg_price_increase  # fraction of income lost
                q5_loss = q5_share * avg_price_increase

                # Redistribution: if fiscal savings given as equal per-capita transfers
                pop = float(pop_vals[-1]) if pop_vals else 1
                per_capita_transfer = pre_tax_total / pop if pop > 0 else 0

                results["welfare_reform"] = {
                    "avg_price_increase_pct": round(float(avg_price_increase) * 100, 1),
                    "q1_budget_share": round(float(q1_share) * 100, 1),
                    "q5_budget_share": round(float(q5_share) * 100, 1),
                    "q1_income_loss_pct": round(float(q1_loss) * 100, 2),
                    "q5_income_loss_pct": round(float(q5_loss) * 100, 2),
                    "progressive_impact": q5_share > q1_share,
                    "per_capita_fiscal_savings": round(float(per_capita_transfer), 2),
                }

        # --- Political economy of reform ---
        reform_difficulty = 0.0
        pe_factors = {}

        # Subsidy size factor
        sub_info = results.get("subsidies", {})
        if sub_info:
            pct_gdp = sub_info.get("pre_tax_pct_gdp", 0) or 0
            size_factor = min(pct_gdp / 5.0, 1.0)  # normalized: 5% GDP = maximum difficulty
            reform_difficulty += size_factor * 30
            pe_factors["subsidy_size_score"] = round(size_factor * 30, 1)

        # Urbanization factor
        if "urban_share" in data:
            urban_vals = list(data["urban_share"].values())
            urban = float(urban_vals[-1]) / 100 if urban_vals else 0.5
            urban_factor = urban  # higher urbanization = more resistance
            reform_difficulty += urban_factor * 20
            pe_factors["urbanization_score"] = round(urban_factor * 20, 1)

        # Institutional capacity
        if "institutional_quality" in data:
            inst_vals = list(data["institutional_quality"].values())
            inst = float(inst_vals[-1]) if inst_vals else 0
            # Higher institutional quality = easier reform (negative relationship)
            inst_factor = max(1 - (inst + 2.5) / 5.0, 0)  # normalize from [-2.5, 2.5]
            reform_difficulty += inst_factor * 25
            pe_factors["institutional_deficit_score"] = round(inst_factor * 25, 1)

        if pe_factors:
            reform_difficulty = float(np.clip(reform_difficulty, 0, 100))
            results["political_economy"] = {
                "reform_difficulty_index": round(reform_difficulty, 1),
                "factors": pe_factors,
                "reform_feasible": reform_difficulty < 50,
            }

        # --- Score ---
        score = 15.0

        # Post-tax subsidy size
        if sub_info:
            pct = sub_info.get("post_tax_pct_gdp", 0) or 0
            score += min(pct * 5, 30)

        # Regressive distribution
        welfare_info = results.get("welfare_reform", {})
        if welfare_info:
            if not welfare_info.get("progressive_impact"):
                score += 10  # regressive: poor pay more

        # Reform difficulty
        pe_info = results.get("political_economy", {})
        if pe_info:
            rd = pe_info.get("reform_difficulty_index", 0)
            score += min(rd * 0.2, 15)

        # Pre-tax subsidy trend (growing?)
        if "oil_con_price" in data and "oil_ref_price" in data:
            common_trend = sorted(set(data["oil_con_price"]) & set(data["oil_ref_price"]))
            if len(common_trend) >= 5:
                gaps = np.array([
                    max(data["oil_ref_price"][d] - data["oil_con_price"][d], 0)
                    for d in common_trend
                ])
                t_arr = np.arange(len(gaps), dtype=float)
                slope, _, _, p, _ = sp_stats.linregress(t_arr, gaps)
                if slope > 0 and p < 0.10:
                    score += 10  # growing subsidy gap

        score = float(np.clip(score, 0, 100))

        return {"score": round(score, 1), "results": results}
