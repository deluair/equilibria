"""Nutrition economics: stunting, wasting, micronutrient deficiency, interventions.

Models child malnutrition (stunting, wasting) prevalence as a function of
income and food prices. Estimates income-nutrition (Engel) elasticities.
Computes economic costs of micronutrient deficiencies. Evaluates cost-
effectiveness of nutrition interventions using DALY-based metrics.

Key references:
    Behrman, J.R., Alderman, H. & Hoddinott, J. (2004). Hunger and
        malnutrition. In Lomborg (ed.), Global Crises, Global Solutions.
        Cambridge University Press.
    Alderman, H., Hoddinott, J. & Kinsey, B. (2006). Long term consequences
        of early childhood malnutrition. Oxford Economic Papers, 58(3).
    Bhutta, Z. et al. (2013). Evidence-based interventions for improvement
        of maternal and child nutrition. Lancet, 382(9890), 452-477.
    Horton, S. & Steckel, R.H. (2013). Malnutrition: global economic losses
        attributable to malnutrition 1900-2000. In Lomborg (ed.), How Much
        Have Global Problems Cost the World?
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class NutritionEconomics(LayerBase):
    layer_id = "l8"
    name = "Nutrition Economics"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Model nutrition outcomes and intervention cost-effectiveness.

        Fetches child malnutrition indicators (stunting, wasting, underweight),
        GDP per capita, food price indices, and dietary energy supply. Estimates
        income-nutrition elasticities, computes economic cost of malnutrition,
        and evaluates cost-effectiveness of standard interventions.

        Returns dict with score, malnutrition modeling, income-nutrition
        elasticity, deficiency costs, and intervention cost-effectiveness.
        """
        country_iso3 = kwargs.get("country_iso3")

        # Stunting prevalence (% of children under 5)
        stunt_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.STA.STNT.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Wasting prevalence (% of children under 5)
        waste_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.STA.WAST.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Underweight prevalence
        uw_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.STA.MALN.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # GDP per capita (constant USD)
        gdppc_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Food price inflation (CPI food index)
        food_cpi_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'FP.CPI.TOTL.ZG'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Prevalence of anemia in women
        anemia_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.ANM.ALLW.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not stunt_rows and not gdppc_rows:
            return {"score": 50, "results": {"error": "no nutrition or GDP data"}}

        def _index(rows):
            out: dict[str, dict[str, float]] = {}
            for r in rows:
                out.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]
            return out

        stunt_data = _index(stunt_rows) if stunt_rows else {}
        waste_data = _index(waste_rows) if waste_rows else {}
        uw_data = _index(uw_rows) if uw_rows else {}
        gdppc_data = _index(gdppc_rows) if gdppc_rows else {}
        anemia_data = _index(anemia_rows) if anemia_rows else {}

        # --- Income-nutrition elasticity ---
        # Cross-country: log(stunting) = a + b*log(GDPpc)
        # Expect b < 0 (higher income = less stunting)
        elasticity = None
        stunt_list, gdp_list, iso_list = [], [], []
        for iso in set(stunt_data.keys()) & set(gdppc_data.keys()):
            s_years = stunt_data[iso]
            g_years = gdppc_data[iso]
            common = sorted(set(s_years.keys()) & set(g_years.keys()))
            if common:
                yr = common[-1]
                s_val = s_years[yr]
                g_val = g_years[yr]
                if s_val and s_val > 0 and g_val and g_val > 0:
                    stunt_list.append(np.log(s_val))
                    gdp_list.append(np.log(g_val))
                    iso_list.append(iso)

        if len(stunt_list) >= 20:
            y = np.array(stunt_list)
            x = np.array(gdp_list)
            slope, intercept, r_val, p_val, se = stats.linregress(x, y)

            elasticity = {
                "income_stunting_elasticity": float(slope),
                "se": float(se),
                "p_value": float(p_val),
                "r_squared": float(r_val**2),
                "n_countries": len(stunt_list),
                "interpretation": (
                    "10% income increase associated with "
                    f"{abs(slope * 10):.1f}% {'decrease' if slope < 0 else 'increase'} in stunting"
                ),
            }

            # Target country position
            if country_iso3 and country_iso3 in iso_list:
                idx = iso_list.index(country_iso3)
                predicted = intercept + slope * x[idx]
                residual = y[idx] - predicted
                elasticity["target_residual"] = float(residual)
                elasticity["target_overperforming"] = bool(residual < 0)

        # --- Stunting/wasting prevalence modeling ---
        malnutrition = None
        if country_iso3:
            stunt_ts = stunt_data.get(country_iso3, {})
            waste_ts = waste_data.get(country_iso3, {})
            uw_ts = uw_data.get(country_iso3, {})

            indicators = {}
            for name, ts in [("stunting", stunt_ts), ("wasting", waste_ts), ("underweight", uw_ts)]:
                if ts:
                    yrs = sorted(ts.keys())
                    latest = yrs[-1]
                    val = ts[latest]

                    # Trend
                    trend = None
                    if len(yrs) >= 3:
                        vals = [ts[y] for y in yrs if ts[y] is not None]
                        t_arr = np.arange(len(vals), dtype=float)
                        if len(vals) >= 3:
                            sl, _, r_v, p_v, _ = stats.linregress(t_arr, vals)
                            trend = {
                                "annual_change": float(sl),
                                "p_value": float(p_v),
                                "improving": bool(sl < 0),
                            }

                    # WHO severity thresholds (for stunting)
                    severity = None
                    if name == "stunting":
                        if val >= 40:
                            severity = "very_high"
                        elif val >= 30:
                            severity = "high"
                        elif val >= 20:
                            severity = "medium"
                        else:
                            severity = "low"
                    elif name == "wasting":
                        if val >= 15:
                            severity = "critical"
                        elif val >= 10:
                            severity = "serious"
                        elif val >= 5:
                            severity = "poor"
                        else:
                            severity = "acceptable"

                    indicators[name] = {
                        "year": latest,
                        "prevalence_pct": float(val),
                        "trend": trend,
                        "severity": severity,
                    }

            if indicators:
                malnutrition = indicators

        # --- Micronutrient deficiency costs ---
        # Estimate GDP loss from malnutrition using Horton-Steckel framework.
        # Stunting reduces adult productivity by ~1.4% per 1% prevalence.
        # Iron deficiency (anemia proxy) reduces GDP by 0.5-2% in developing countries.
        deficiency_costs = None
        if country_iso3 and country_iso3 in gdppc_data:
            gdp_years = gdppc_data[country_iso3]
            latest_yr = sorted(gdp_years.keys())[-1]
            costs = {}

            # Stunting productivity loss
            if country_iso3 in stunt_data:
                stunt_yrs = stunt_data[country_iso3]
                if stunt_yrs:
                    s_yr = sorted(stunt_yrs.keys())[-1]
                    s_val = stunt_yrs[s_yr]
                    # Hoddinott et al.: 1% stunting ~ 1.4% lower adult earnings
                    productivity_loss_pct = s_val * 0.014 * 100  # percentage
                    gdp_loss_pct = s_val * 0.014 * 0.5  # aggregate GDP effect (dampened)
                    costs["stunting"] = {
                        "prevalence": float(s_val),
                        "productivity_loss_pct": round(float(productivity_loss_pct), 1),
                        "gdp_loss_pct": round(float(gdp_loss_pct), 2),
                    }

            # Anemia cost
            if country_iso3 in anemia_data:
                an_yrs = anemia_data[country_iso3]
                if an_yrs:
                    a_yr = sorted(an_yrs.keys())[-1]
                    a_val = an_yrs[a_yr]
                    # Horton-Ross: iron deficiency costs 0.5-2% GDP in high-prevalence
                    anemia_gdp_loss = min(a_val * 0.03, 2.5)  # cap at 2.5%
                    costs["anemia"] = {
                        "prevalence_women": float(a_val),
                        "estimated_gdp_loss_pct": round(float(anemia_gdp_loss), 2),
                    }

            if costs:
                total_gdp_loss = sum(
                    c.get("gdp_loss_pct", 0) or c.get("estimated_gdp_loss_pct", 0)
                    for c in costs.values()
                )
                costs["total_estimated_gdp_loss_pct"] = round(total_gdp_loss, 2)
                deficiency_costs = costs

        # --- Cost-effectiveness of interventions ---
        # Based on Bhutta et al. (2013) Lancet Nutrition Series
        # Costs per DALY averted for proven interventions
        interventions = {
            "vitamin_a_supplementation": {
                "cost_per_daly_averted_usd": 7,
                "cost_effectiveness": "very_high",
                "target": "children_6_59_months",
            },
            "zinc_supplementation": {
                "cost_per_daly_averted_usd": 73,
                "cost_effectiveness": "high",
                "target": "children_with_diarrhea",
            },
            "iron_fortification": {
                "cost_per_daly_averted_usd": 66,
                "cost_effectiveness": "high",
                "target": "general_population",
            },
            "breastfeeding_promotion": {
                "cost_per_daly_averted_usd": 12,
                "cost_effectiveness": "very_high",
                "target": "infants_0_6_months",
            },
            "complementary_feeding": {
                "cost_per_daly_averted_usd": 53,
                "cost_effectiveness": "high",
                "target": "children_6_23_months",
            },
            "iodine_fortification": {
                "cost_per_daly_averted_usd": 34,
                "cost_effectiveness": "very_high",
                "target": "iodine_deficient_areas",
            },
        }

        # Prioritize interventions for target country based on burden
        intervention_priority = None
        if malnutrition and deficiency_costs:
            priority_list = []
            if "stunting" in malnutrition and malnutrition["stunting"]["prevalence_pct"] > 20:
                priority_list.extend(["complementary_feeding", "breastfeeding_promotion"])
            if deficiency_costs.get("anemia", {}).get("prevalence_women", 0) > 30:
                priority_list.extend(["iron_fortification"])
            # Always include most cost-effective
            priority_list.extend(["vitamin_a_supplementation", "iodine_fortification"])
            seen = set()
            unique = []
            for p in priority_list:
                if p not in seen:
                    unique.append(p)
                    seen.add(p)
            intervention_priority = [
                {"intervention": name, **interventions[name]}
                for name in unique
                if name in interventions
            ]

        # --- Score ---
        score = 30
        if malnutrition:
            if "stunting" in malnutrition:
                sev = malnutrition["stunting"].get("severity")
                if sev == "very_high":
                    score += 35
                elif sev == "high":
                    score += 25
                elif sev == "medium":
                    score += 15
            if "wasting" in malnutrition:
                sev = malnutrition["wasting"].get("severity")
                if sev == "critical":
                    score += 20
                elif sev == "serious":
                    score += 12

        if deficiency_costs:
            total_loss = deficiency_costs.get("total_estimated_gdp_loss_pct", 0)
            if total_loss > 3:
                score += 10
            elif total_loss > 1:
                score += 5

        score = float(np.clip(score, 0, 100))

        results = {
            "income_nutrition_elasticity": elasticity,
            "malnutrition": malnutrition,
            "deficiency_costs": deficiency_costs,
            "intervention_cost_effectiveness": interventions,
            "intervention_priority": intervention_priority,
            "country_iso3": country_iso3,
        }

        return {"score": score, "results": results}
