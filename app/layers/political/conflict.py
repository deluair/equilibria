"""Collier-Hoeffler greed vs grievance model and conflict economics.

Collier & Hoeffler (2004) model civil war onset as a function of economic
opportunity for rebellion ("greed") vs political/ethnic grievances:

    P(war) = f(primary_exports/GDP, diaspora, male_secondary_enrollment,
               growth, ethnic_fractionalization, religious_polarization,
               population, mountainous_terrain, peace_duration)

Key findings: primary commodity dependence (proxy for lootable resources)
and low income/growth are stronger predictors than ethnic/religious
grievance variables. Resource curse: Sachs & Warner (2001) show resource-
rich countries grow slower, partly through conflict risk.

Economic costs of conflict (Collier 1999):
    - GDP declines ~2.2% per year during civil war
    - Capital flight, brain drain, infrastructure destruction
    - Neighborhood spillovers: each civil war reduces neighbor GDP growth by 0.5pp

Post-conflict recovery (Cerra & Saxena 2008):
    - Recovery is typically incomplete: permanent output loss of 5-12%
    - Growth rebounds quickly but level never fully recovers

Score: high conflict risk factors + active/recent conflict + resource
dependence -> high stress.

References:
    Collier, P. & Hoeffler, A. (2004). "Greed and Grievance in Civil War."
        Oxford Economic Papers 56(4).
    Collier, P. (1999). "On the Economic Consequences of Civil War." Oxford
        Economic Papers 51(1).
    Cerra, V. & Saxena, S.C. (2008). "Growth Dynamics: The Myth of Economic
        Recovery." AER 98(1).
    Sachs, J. & Warner, A. (2001). "The Curse of Natural Resources." European
        Economic Review 45(4-6).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ConflictEconomics(LayerBase):
    layer_id = "l12"
    name = "Conflict Economics"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate Collier-Hoeffler conflict risk and economic costs.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 code (default BGD)
        """
        country = kwargs.get("country_iso3", "BGD")

        # Fetch conflict/fragility indicators
        conflict_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%conflict%' OR ds.name LIKE '%fragil%'
                   OR ds.name LIKE '%battle%death%' OR ds.name LIKE '%political%stability%'
                   OR ds.name LIKE '%violence%' OR ds.name LIKE '%peace%index%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Fetch Collier-Hoeffler risk factors
        risk_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%primary%exports%' OR ds.name LIKE '%natural%resource%rent%'
                   OR ds.name LIKE '%ethnic%fractionalization%' OR ds.name LIKE '%secondary%enrollment%'
                   OR ds.name LIKE '%population%' OR ds.name LIKE '%mountainous%terrain%'
                   OR ds.name LIKE '%gdp%per%capita%')
            ORDER BY dp.date DESC
            LIMIT 50
            """,
            (country,),
        )

        # Fetch GDP growth for cost estimation
        growth_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('wdi', 'imf', 'fred')
              AND (ds.name LIKE '%gdp%growth%' OR ds.name LIKE '%real gdp%growth%')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not conflict_rows and not risk_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no conflict/risk factor data"}

        # --- Parse conflict indicators ---
        political_stability = None

        if conflict_rows:
            for r in conflict_rows:
                name = r["name"].lower()
                val = float(r["value"]) if r["value"] is not None else None
                if val is None:
                    continue
                if "political" in name and "stability" in name:
                    political_stability = val

        # --- Collier-Hoeffler risk factors ---
        risk_factors = {}
        for r in risk_rows:
            name = r["name"].lower()
            val = float(r["value"]) if r["value"] is not None else None
            if val is None:
                continue
            if "primary" in name and "export" in name:
                risk_factors["primary_exports_gdp"] = val
            elif "natural" in name and "resource" in name and "rent" in name:
                risk_factors["resource_rents_gdp"] = val
            elif "ethnic" in name and "fractionalization" in name:
                risk_factors["ethnic_fractionalization"] = val
            elif "secondary" in name and "enrollment" in name:
                risk_factors["male_secondary_enrollment"] = val
            elif "gdp" in name and "per" in name and "capita" in name:
                risk_factors["gdp_per_capita"] = val

        # --- Collier-Hoeffler conflict probability (simplified logit) ---
        # P(conflict) increases with resource dependence, low income, low education
        ch_probability = None
        if risk_factors:
            # Normalized risk contributions (0-1 each)
            resource_risk = 0.0
            income_risk = 0.0
            education_risk = 0.0

            if "primary_exports_gdp" in risk_factors:
                # CH: peak risk at ~33% of GDP (inverted U)
                pex = risk_factors["primary_exports_gdp"]
                resource_risk = 4 * pex * (1 - pex) if 0 <= pex <= 1 else min(pex / 33.0, 1.0)
            elif "resource_rents_gdp" in risk_factors:
                rr = risk_factors["resource_rents_gdp"]
                resource_risk = min(rr / 30.0, 1.0)  # 30% = max risk proxy

            if "gdp_per_capita" in risk_factors:
                gdppc = risk_factors["gdp_per_capita"]
                # Low income -> high risk; threshold ~$5000
                income_risk = max(0.0, 1.0 - gdppc / 5000.0) if gdppc > 0 else 0.5

            if "male_secondary_enrollment" in risk_factors:
                enroll = risk_factors["male_secondary_enrollment"]
                # Low enrollment -> high risk
                education_risk = max(0.0, 1.0 - enroll / 100.0) if enroll >= 0 else 0.5

            # Weighted combination (CH weights approximate)
            ch_probability = float(np.clip(
                0.15 * resource_risk + 0.30 * income_risk + 0.20 * education_risk + 0.05, 0, 1
            ))

            # Ethnic fractionalization modifier
            if "ethnic_fractionalization" in risk_factors:
                ef = risk_factors["ethnic_fractionalization"]
                # CH: moderate fractionalization increases risk, very high may reduce
                # (dominant group can suppress)
                frac_modifier = 4 * ef * (1 - ef) if 0 <= ef <= 1 else 0
                ch_probability = float(np.clip(ch_probability + 0.10 * frac_modifier, 0, 1))

        # --- Economic costs of conflict ---
        conflict_costs = None
        if growth_rows and len(growth_rows) >= 5:
            g_vals = np.array([float(r["value"]) for r in growth_rows])
            g_dates = [r["date"] for r in growth_rows]
            g_years = np.array([int(str(d)[:4]) for d in g_dates])

            # Identify conflict periods (growth < -2% as proxy or use conflict indicators)
            mean_growth = float(np.mean(g_vals))
            crisis_mask = g_vals < -2.0

            if crisis_mask.sum() > 0:
                crisis_years = g_years[crisis_mask]
                crisis_growth = g_vals[crisis_mask]
                normal_growth = g_vals[~crisis_mask]

                # Cumulative GDP loss during conflict episodes
                growth_gap = mean_growth - np.mean(crisis_growth)
                cumulative_loss = float(growth_gap * crisis_mask.sum())

                # Post-crisis recovery: check if growth rebounds
                recovery_years = []
                for cy in crisis_years:
                    post = g_vals[(g_years > cy) & (g_years <= cy + 5)]
                    if len(post) > 0:
                        recovery_years.append(float(np.mean(post)))

                conflict_costs = {
                    "mean_crisis_growth": round(float(np.mean(crisis_growth)), 4),
                    "mean_normal_growth": round(float(np.mean(normal_growth)), 4) if normal_growth.size > 0 else None,
                    "growth_gap": round(float(growth_gap), 4),
                    "n_crisis_years": int(crisis_mask.sum()),
                    "cumulative_gdp_loss_pct": round(cumulative_loss, 2),
                    "recovery_avg_growth": round(float(np.mean(recovery_years)), 4) if recovery_years else None,
                    "collier_benchmark": -2.2,  # Collier (1999): -2.2% per war year
                }

        # --- Resource curse channel ---
        resource_curse = None
        if "resource_rents_gdp" in risk_factors and growth_rows and len(growth_rows) >= 5:
            rr = risk_factors["resource_rents_gdp"]
            g_mean = float(np.mean([float(r["value"]) for r in growth_rows]))
            # Sachs-Warner: high resource rents correlate with lower growth
            resource_curse = {
                "resource_rents_gdp": round(rr, 2),
                "mean_growth": round(g_mean, 2),
                "curse_likely": rr > 10 and g_mean < 3.0,
                "note": "Sachs-Warner: resource-rich countries grow slower, partly via conflict",
            }

        # --- Neighborhood spillover (Collier) ---
        # Fetch neighbor conflict data
        neighbor_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.country_iso3
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.name LIKE '%conflict%intensity%'
              AND ds.country_iso3 != ?
            ORDER BY dp.date DESC
            LIMIT 20
            """,
            (country,),
        )

        spillover = None
        if neighbor_rows:
            neighbor_vals = [float(r["value"]) for r in neighbor_rows if r["value"] is not None]
            if neighbor_vals:
                spillover = {
                    "n_neighbors_with_conflict": len([v for v in neighbor_vals if v > 0]),
                    "mean_neighbor_intensity": round(float(np.mean(neighbor_vals)), 2),
                    "estimated_growth_spillover": round(-0.5 * len([v for v in neighbor_vals if v > 0]), 2),
                    "note": "Collier: each neighboring civil war reduces growth by ~0.5pp",
                }

        # --- Score ---
        score_parts = []

        # Conflict probability component (0-40)
        if ch_probability is not None:
            score_parts.append(ch_probability * 40.0)
        else:
            score_parts.append(20.0)

        # Political stability (WGI scale -2.5 to 2.5; lower = less stable)
        if political_stability is not None:
            # Map -2.5 to 2.5 -> 30 to 0
            stab_score = max(0.0, (2.5 - political_stability) / 5.0 * 30.0)
            score_parts.append(stab_score)
        else:
            score_parts.append(15.0)

        # Conflict costs / resource curse (0-30)
        if conflict_costs and conflict_costs["n_crisis_years"] > 0:
            cost_score = min(30.0, conflict_costs["n_crisis_years"] * 5.0)
            score_parts.append(cost_score)
        elif resource_curse and resource_curse["curse_likely"]:
            score_parts.append(20.0)
        else:
            score_parts.append(5.0)

        score = float(np.clip(sum(score_parts), 0, 100))

        result = {
            "score": round(score, 2),
            "country": country,
            "risk_factors": risk_factors,
        }

        if ch_probability is not None:
            result["collier_hoeffler_probability"] = round(ch_probability, 4)
        if political_stability is not None:
            result["political_stability"] = round(political_stability, 4)
        if conflict_costs:
            result["economic_costs"] = conflict_costs
        if resource_curse:
            result["resource_curse"] = resource_curse
        if spillover:
            result["neighborhood_spillover"] = spillover

        return result
