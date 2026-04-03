"""Tit-for-tat tariff escalation and optimal tariff theory.

Optimal tariff (Johnson 1953): a large country can improve its terms of trade
by imposing a tariff. The optimal tariff rate is:

    t* = 1 / (epsilon_s - 1)

where epsilon_s is the foreign export supply elasticity. Small epsilon_s
(inelastic supply) -> large optimal tariff. The welfare gain comes from
terms-of-trade improvement minus deadweight loss.

Tit-for-tat escalation: when both countries retaliate, the Nash equilibrium
tariff exceeds the cooperative (free trade) outcome. The trade war game is
a Prisoner's Dilemma:

    Payoff matrix (welfare):
                        Country B
                    Free Trade  |  Tariff
    Country A  FT  |  (W*, W*)    (L, W*+G)
               T   |  (W*+G, L)  (W*-D, W*-D)

where G = terms of trade gain, D = deadweight loss + retaliation cost.
Both countries tariffing (Nash) is Pareto-inferior to free trade.

Third-party spillovers (Ossa 2014): bilateral trade wars cause trade
diversion to third parties. Some third parties gain (trade deflection),
others lose (supply chain disruption).

References:
    Johnson, H. (1953). "Optimum Tariffs and Retaliation." RES 21(2).
    Ossa, R. (2014). "Trade Wars and Trade Talks with Data." AER 104(12).
    Amiti, M., Redding, S. & Weinstein, D. (2019). "The Impact of the 2018
        Tariffs on Prices and Welfare." JEP 33(4).
    Fajgelbaum, P. et al. (2020). "The Return to Protectionism." QJE 135(1).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TradeWarAnalysis(LayerBase):
    layer_id = "l12"
    name = "Trade War Analysis"

    async def compute(self, db, **kwargs) -> dict:
        """Model trade war escalation, optimal tariffs, and spillovers.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            reporter : str - ISO3 of country A (default USA)
            partner : str - ISO3 of country B (default CHN)
            year : int - reference year
        """
        reporter = kwargs.get("reporter", "USA")
        partner = kwargs.get("partner", "CHN")
        year = kwargs.get("year", 2022)

        # Fetch bilateral trade data
        bilateral_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND CAST(SUBSTR(dp.date, 1, 4) AS INTEGER) BETWEEN ? AND ?
              AND ds.source IN ('comtrade', 'wdi', 'fred')
              AND (ds.name LIKE '%export%' OR ds.name LIKE '%import%'
                   OR ds.name LIKE '%tariff%' OR ds.name LIKE '%trade%value%')
            ORDER BY dp.date
            """,
            (reporter, year - 10, year),
        )

        # Fetch tariff data
        tariff_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ((ds.country_iso3 = ? AND ds.name LIKE '%tariff%' AND ds.metadata LIKE ?)
                OR (ds.country_iso3 = ? AND ds.name LIKE '%tariff%' AND ds.metadata LIKE ?))
            ORDER BY dp.date
            """,
            (reporter, f"%{partner}%", partner, f"%{reporter}%"),
        )

        # Fetch supply/demand elasticities
        elasticity_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.name, ds.country_iso3
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 IN (?, ?)
              AND (ds.name LIKE '%import%elasticity%' OR ds.name LIKE '%export%supply%elasticity%'
                   OR ds.name LIKE '%trade%elasticity%' OR ds.name LIKE '%armington%')
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (reporter, partner),
        )

        if not bilateral_rows and not tariff_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no bilateral trade/tariff data"}

        # --- Parse bilateral trade ---
        exports_ts = {}
        imports_ts = {}
        for r in bilateral_rows:
            name = r["name"].lower()
            yr = int(str(r["date"])[:4])
            val = float(r["value"]) if r["value"] is not None else 0
            if "export" in name:
                exports_ts[yr] = val
            elif "import" in name:
                imports_ts[yr] = val

        # --- Parse tariff escalation ---
        tariff_a = {}  # Reporter's tariffs on partner
        tariff_b = {}  # Partner's tariffs on reporter
        for r in tariff_rows:
            yr = int(str(r["date"])[:4])
            val = float(r["value"]) if r["value"] is not None else 0
            iso = r.get("country_iso3", reporter) if hasattr(r, "get") else reporter
            # Heuristic: assign based on country_iso3
            name = (r["name"] or "").lower()
            meta = (r["metadata"] or "").lower()
            if reporter.lower() in meta or reporter.lower() in name:
                tariff_b[yr] = val
            else:
                tariff_a[yr] = val

        # --- Optimal tariff calculation ---
        # Parse elasticities
        supply_elasticity_b = None  # Partner's export supply elasticity
        for r in elasticity_rows:
            val = float(r["value"]) if r["value"] is not None else None
            if val is not None and val > 0:
                name = r["name"].lower()
                iso = r["country_iso3"]
                if iso == partner and ("supply" in name or "export" in name):
                    supply_elasticity_b = val
                elif iso == reporter and ("import" in name or "demand" in name):
                    pass  # Could use for welfare calc

        optimal_tariff = None
        if supply_elasticity_b is not None and supply_elasticity_b > 1.0:
            t_star = 1.0 / (supply_elasticity_b - 1.0)
            optimal_tariff = {
                "optimal_rate_pct": round(t_star * 100, 2),
                "supply_elasticity": round(supply_elasticity_b, 4),
                "formula": "t* = 1 / (epsilon_s - 1)",
                "note": "Johnson (1953) optimal tariff for terms-of-trade gain",
            }
        elif supply_elasticity_b is not None:
            optimal_tariff = {
                "optimal_rate_pct": None,
                "supply_elasticity": round(supply_elasticity_b, 4),
                "note": "Supply elasticity <= 1: optimal tariff undefined (prohibitive)",
            }

        # --- Tit-for-tat escalation detection ---
        escalation = None
        if tariff_a and tariff_b:
            common_years = sorted(set(tariff_a.keys()) & set(tariff_b.keys()))
            if len(common_years) >= 3:
                ta = np.array([tariff_a[y] for y in common_years])
                tb = np.array([tariff_b[y] for y in common_years])

                # Detect escalation: both tariffs trending up
                t_idx = np.arange(len(common_years), dtype=float)

                from scipy.stats import linregress
                slope_a, _, _, p_a, _ = linregress(t_idx, ta)
                slope_b, _, _, p_b, _ = linregress(t_idx, tb)

                # Cross-correlation: does B respond to A's tariff increases?
                if len(ta) >= 4:
                    # Lag B by 1 period, correlate with A
                    corr_coeff = float(np.corrcoef(ta[:-1], tb[1:])[0, 1]) if len(ta) > 1 else 0.0
                else:
                    corr_coeff = 0.0

                escalation = {
                    "reporter_tariff_trend": round(float(slope_a), 4),
                    "partner_tariff_trend": round(float(slope_b), 4),
                    "reporter_trend_p": round(float(p_a), 4),
                    "partner_trend_p": round(float(p_b), 4),
                    "tit_for_tat_correlation": round(corr_coeff, 4),
                    "escalation_detected": slope_a > 0 and slope_b > 0 and corr_coeff > 0.3,
                    "latest_reporter_tariff": round(float(ta[-1]), 2),
                    "latest_partner_tariff": round(float(tb[-1]), 2),
                    "years": common_years,
                }

        # --- Welfare analysis (Prisoner's Dilemma payoffs) ---
        welfare = None
        if exports_ts and imports_ts:
            latest_exports = list(exports_ts.values())[-1] if exports_ts else 0
            latest_imports = list(imports_ts.values())[-1] if imports_ts else 0
            total_bilateral = latest_exports + latest_imports

            if total_bilateral > 0:
                # Amiti et al. (2019): US tariffs on China cost US consumers ~$51B/year
                # on ~$300B imports. Welfare cost ~ tariff_rate * import_value * (1 + 0.5*elasticity*tariff)
                avg_tariff_a = float(np.mean(list(tariff_a.values()))) / 100 if tariff_a else 0.05
                avg_tariff_b = float(np.mean(list(tariff_b.values()))) / 100 if tariff_b else 0.05

                # Deadweight loss (Harberger triangle): 0.5 * t^2 * elasticity * import_value
                elasticity_est = supply_elasticity_b if supply_elasticity_b else 3.0  # Default Armington
                dwl_a = 0.5 * avg_tariff_a ** 2 * elasticity_est * latest_imports
                dwl_b = 0.5 * avg_tariff_b ** 2 * elasticity_est * latest_exports

                # Terms of trade gain: t / (1+t) * import_value * (1 - 1/epsilon)
                if supply_elasticity_b and supply_elasticity_b > 1:
                    tot_gain_a = (avg_tariff_a / (1 + avg_tariff_a)) * latest_imports * (1 - 1 / supply_elasticity_b)
                else:
                    tot_gain_a = 0

                net_welfare_a = tot_gain_a - dwl_a

                welfare = {
                    "bilateral_trade_value": round(total_bilateral, 0),
                    "reporter_avg_tariff_pct": round(avg_tariff_a * 100, 2),
                    "partner_avg_tariff_pct": round(avg_tariff_b * 100, 2),
                    "reporter_deadweight_loss": round(dwl_a, 0),
                    "partner_deadweight_loss": round(dwl_b, 0),
                    "reporter_tot_gain": round(tot_gain_a, 0),
                    "reporter_net_welfare": round(net_welfare_a, 0),
                    "mutual_tariff_outcome": "both lose" if net_welfare_a < 0 else "reporter gains net",
                    "prisoners_dilemma": net_welfare_a < 0,
                }

        # --- Third-party spillovers ---
        spillover_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 NOT IN (?, ?)
              AND ds.name LIKE '%trade%diversion%'
              AND CAST(SUBSTR(dp.date, 1, 4) AS INTEGER) = ?
            ORDER BY dp.value DESC
            LIMIT 10
            """,
            (reporter, partner, year),
        )

        spillovers = None
        if spillover_rows:
            winners = []
            losers = []
            for r in spillover_rows:
                val = float(r["value"]) if r["value"] is not None else 0
                entry = {"country": r["country_iso3"], "trade_change": round(val, 2)}
                if val > 0:
                    winners.append(entry)
                else:
                    losers.append(entry)
            spillovers = {
                "winners": winners[:5],
                "losers": losers[:5],
                "note": "Ossa (2014): bilateral trade wars cause third-party diversion",
            }

        # --- Score ---
        score_parts = []

        # Escalation severity (0-40)
        if escalation and escalation["escalation_detected"]:
            esc_score = min(40.0, 20.0 + escalation["tit_for_tat_correlation"] * 20.0)
            score_parts.append(esc_score)
        elif tariff_a or tariff_b:
            max_tariff = max(
                max(tariff_a.values()) if tariff_a else 0,
                max(tariff_b.values()) if tariff_b else 0,
            )
            score_parts.append(min(30.0, max_tariff * 1.5))
        else:
            score_parts.append(5.0)

        # Welfare cost (0-35)
        if welfare:
            if welfare["prisoners_dilemma"]:
                score_parts.append(30.0)
            elif welfare["reporter_deadweight_loss"] > 0:
                score_parts.append(15.0)
            else:
                score_parts.append(5.0)
        else:
            score_parts.append(10.0)

        # Trade disruption (0-25)
        if exports_ts and len(exports_ts) >= 3:
            exp_vals = np.array(list(exports_ts.values()))
            if len(exp_vals) >= 3:
                # Trade volume decline
                recent_change = (exp_vals[-1] - exp_vals[-3]) / exp_vals[-3] if exp_vals[-3] > 0 else 0
                if recent_change < -0.10:
                    score_parts.append(25.0)
                elif recent_change < 0:
                    score_parts.append(15.0)
                else:
                    score_parts.append(5.0)
            else:
                score_parts.append(10.0)
        else:
            score_parts.append(10.0)

        score = float(np.clip(sum(score_parts), 0, 100))

        result = {
            "score": round(score, 2),
            "reporter": reporter,
            "partner": partner,
            "year": year,
        }

        if optimal_tariff:
            result["optimal_tariff"] = optimal_tariff
        if escalation:
            result["tariff_escalation"] = escalation
        if welfare:
            result["welfare_analysis"] = welfare
        if spillovers:
            result["third_party_spillovers"] = spillovers

        return result
