"""Hufbauer sanctions effectiveness scoring and GDP impact.

Hufbauer, Schott, Elliott & Oegg (2007) built the canonical sanctions
database (HSEO). Each sanctions episode is scored on:

    Policy result (1-4): 1 = failed, 4 = achieved policy goal
    Sanctions contribution (1-4): 1 = negative, 4 = decisive
    Success score = policy_result * sanctions_contribution / 16

Empirically, ~34% of episodes score >= 9/16 ("success"). Success correlates
with: modest goals, target vulnerability, strong sender-target trade ties,
multilateral support, and financial (not trade) sanctions.

GDP impact (Neuenkirch & Neumeier 2015):
    UN sanctions reduce target GDP growth by ~2.3-3.5 pp/year
    US-only sanctions reduce by ~0.5-0.9 pp/year
    EU-only sanctions: similar to US, ~0.4-1.0 pp/year

Smart sanctions (Targeted Financial Sanctions, TFS): asset freezes,
travel bans, arms embargoes. Less collateral damage than comprehensive
trade embargoes but also less effective at coercion.

Humanitarian exemptions: carve-outs for food, medicine, and humanitarian
goods. The cost of exemptions is the administrative burden and potential
sanctions leakage.

Score: high GDP impact + broad sanctions + low exemption coverage -> stress.

References:
    Hufbauer, G.C. et al. (2007). Economic Sanctions Reconsidered, 3rd ed.
        Peterson Institute.
    Neuenkirch, M. & Neumeier, F. (2015). "The Impact of UN and US Economic
        Sanctions on GDP Growth." European Journal of Political Economy 40.
    Drezner, D. (1999). The Sanctions Paradox. Cambridge University Press.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class SanctionsEconomics(LayerBase):
    layer_id = "l12"
    name = "Sanctions Economics"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate sanctions effectiveness and GDP impact.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 code of target country (default IRN)
            sender : str - sender ISO3 (default USA)
        """
        country = kwargs.get("country_iso3", "IRN")
        sender = kwargs.get("sender", "USA")

        # Fetch sanctions indicators
        sanctions_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%sanction%' OR ds.name LIKE '%embargo%'
                   OR ds.name LIKE '%asset%freeze%' OR ds.name LIKE '%travel%ban%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Fetch GDP growth for impact estimation
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

        # Fetch trade data for trade dependence on sender
        trade_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%trade%' OR ds.name LIKE '%export%' OR ds.name LIKE '%import%')
              AND ds.source IN ('comtrade', 'wdi')
            ORDER BY dp.date DESC
            LIMIT 20
            """,
            (country,),
        )

        if not sanctions_rows and not growth_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no sanctions/growth data"}

        # --- Parse sanctions episodes ---
        sanctions_active = False
        sanctions_types = set()
        sanctions_years = []

        if sanctions_rows:
            for r in sanctions_rows:
                name = r["name"].lower()
                val = float(r["value"]) if r["value"] is not None else 0
                if val > 0:
                    sanctions_active = True
                    year = int(str(r["date"])[:4])
                    sanctions_years.append(year)
                    if "embargo" in name:
                        sanctions_types.add("trade_embargo")
                    elif "asset" in name and "freeze" in name:
                        sanctions_types.add("asset_freeze")
                    elif "travel" in name and "ban" in name:
                        sanctions_types.add("travel_ban")
                    else:
                        sanctions_types.add("general")

        # --- Hufbauer effectiveness scoring ---
        hufbauer_score = None
        if sanctions_rows:
            # Look for policy result and contribution scores in data
            policy_result = None
            sanctions_contribution = None
            for r in sanctions_rows:
                name = r["name"].lower()
                val = float(r["value"]) if r["value"] is not None else None
                if val is None:
                    continue
                if "policy" in name and "result" in name:
                    policy_result = val
                elif "contribution" in name:
                    sanctions_contribution = val

            if policy_result is not None and sanctions_contribution is not None:
                raw_score = (policy_result * sanctions_contribution) / 16.0
                hufbauer_score = {
                    "policy_result": round(policy_result, 1),
                    "sanctions_contribution": round(sanctions_contribution, 1),
                    "composite_score": round(raw_score, 4),
                    "success": raw_score >= 9.0 / 16.0,
                    "threshold": round(9.0 / 16.0, 4),
                }
            elif sanctions_active:
                # Estimate from duration and type
                duration = len(set(sanctions_years))
                # Longer sanctions less likely to succeed (Drezner paradox)
                est_success_prob = max(0.1, 0.5 - 0.03 * duration)
                hufbauer_score = {
                    "estimated_success_probability": round(est_success_prob, 4),
                    "duration_years": duration,
                    "note": "Drezner paradox: longer sanctions less effective",
                }

        # --- GDP impact estimation ---
        gdp_impact = None
        if growth_rows and len(growth_rows) >= 5:
            g_dates = [r["date"] for r in growth_rows]
            g_vals = np.array([float(r["value"]) for r in growth_rows])
            g_years = np.array([int(str(d)[:4]) for d in g_dates])

            if sanctions_years:
                sanctions_set = set(sanctions_years)
                sanctioned_mask = np.array([y in sanctions_set for y in g_years])

                if sanctioned_mask.sum() > 0 and (~sanctioned_mask).sum() > 0:
                    mean_sanctioned = float(np.mean(g_vals[sanctioned_mask]))
                    mean_unsanctioned = float(np.mean(g_vals[~sanctioned_mask]))
                    growth_gap = mean_sanctioned - mean_unsanctioned

                    # Welch t-test
                    t_stat, p_val = stats.ttest_ind(
                        g_vals[sanctioned_mask], g_vals[~sanctioned_mask], equal_var=False
                    )

                    gdp_impact = {
                        "mean_growth_during_sanctions": round(mean_sanctioned, 4),
                        "mean_growth_without_sanctions": round(mean_unsanctioned, 4),
                        "growth_gap_pp": round(growth_gap, 4),
                        "t_statistic": round(float(t_stat), 4),
                        "p_value": round(float(p_val), 4),
                        "significant_at_10pct": float(p_val) < 0.10,
                        "n_sanctioned_years": int(sanctioned_mask.sum()),
                        "n_unsanctioned_years": int((~sanctioned_mask).sum()),
                        "neuenkirch_benchmark_un": {"low": -3.5, "high": -2.3},
                        "neuenkirch_benchmark_us": {"low": -0.9, "high": -0.5},
                    }
            else:
                # No specific sanctions periods identified; use overall growth analysis
                mean_g = float(np.mean(g_vals))
                std_g = float(np.std(g_vals))
                gdp_impact = {
                    "mean_growth": round(mean_g, 4),
                    "std_growth": round(std_g, 4),
                    "note": "No sanctions periods identified for diff-in-diff",
                }

        # --- Smart sanctions assessment ---
        smart_sanctions = None
        if sanctions_types:
            targeted = sanctions_types & {"asset_freeze", "travel_ban"}
            comprehensive = sanctions_types & {"trade_embargo"}
            smart_sanctions = {
                "types_active": sorted(sanctions_types),
                "is_targeted": len(targeted) > 0 and len(comprehensive) == 0,
                "is_comprehensive": len(comprehensive) > 0,
                "expected_collateral": "high" if comprehensive else "low" if targeted else "moderate",
                "expected_effectiveness": "moderate" if comprehensive else "low" if len(targeted) == 1 else "moderate",
            }

        # --- Trade dependence on sender ---
        trade_dependence = None
        if trade_rows:
            trade_vals = [float(r["value"]) for r in trade_rows if r["value"] is not None]
            if trade_vals:
                total_trade = sum(trade_vals)
                # Proxy: if sender is major partner, sanctions bite harder
                trade_dependence = {
                    "total_trade_latest": round(total_trade, 0),
                    "note": f"Higher trade dependence on {sender} increases sanctions impact",
                }

        # --- Humanitarian exemption cost estimate ---
        # Simplified: exemptions reduce sanctions effectiveness by 10-30%
        # but also reduce GDP cost by similar amount
        humanitarian_cost = None
        if gdp_impact and "growth_gap_pp" in gdp_impact:
            gap = abs(gdp_impact["growth_gap_pp"])
            # Exemptions mitigate 20% of impact (mid estimate)
            mitigated = gap * 0.20
            humanitarian_cost = {
                "estimated_mitigation_pp": round(mitigated, 4),
                "admin_cost_note": "Humanitarian exemptions add compliance costs but reduce civilian harm",
                "leakage_risk": "moderate" if gap > 2.0 else "low",
            }

        # --- Score ---
        score_parts = []

        # Sanctions severity (0-40)
        if sanctions_active:
            if "trade_embargo" in sanctions_types:
                score_parts.append(35.0)
            elif len(sanctions_types) >= 2:
                score_parts.append(25.0)
            else:
                score_parts.append(15.0)
        else:
            score_parts.append(0.0)

        # GDP impact magnitude (0-35)
        if gdp_impact and "growth_gap_pp" in gdp_impact:
            gap = abs(gdp_impact["growth_gap_pp"])
            impact_score = float(np.clip(gap * 10.0, 0, 35))
            score_parts.append(impact_score)
        else:
            score_parts.append(5.0 if sanctions_active else 0.0)

        # Duration penalty (0-25)
        if sanctions_years:
            duration = len(set(sanctions_years))
            duration_score = float(np.clip(duration * 2.5, 0, 25))
            score_parts.append(duration_score)
        else:
            score_parts.append(0.0)

        score = float(np.clip(sum(score_parts), 0, 100))

        result = {
            "score": round(score, 2),
            "country": country,
            "sender": sender,
            "sanctions_active": sanctions_active,
            "sanctions_types": sorted(sanctions_types) if sanctions_types else [],
            "n_sanctions_years": len(set(sanctions_years)) if sanctions_years else 0,
        }

        if hufbauer_score:
            result["hufbauer_effectiveness"] = hufbauer_score
        if gdp_impact:
            result["gdp_impact"] = gdp_impact
        if smart_sanctions:
            result["smart_sanctions"] = smart_sanctions
        if trade_dependence:
            result["trade_dependence"] = trade_dependence
        if humanitarian_cost:
            result["humanitarian_exemption"] = humanitarian_cost

        return result
