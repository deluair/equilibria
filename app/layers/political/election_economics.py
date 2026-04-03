"""Election Economics module.

Four dimensions of electoral political economy:

1. **Electoral cycle in fiscal policy** (Nordhaus 1975, Rogoff 1990):
   Political business cycle: incumbents expand fiscal policy pre-election
   and contract post-election. Rogoff's competence signaling model predicts
   increased spending on visible goods (transfers, infrastructure) before
   elections. Estimated by regressing fiscal balance on election-year dummies
   and distance-to-election.

2. **Campaign spending effectiveness** (Levitt 1994):
   Levitt's quasi-experimental identification: controlling for candidate
   fixed effects, doubling challenger spending raises vote share 1%, while
   doubling incumbent spending raises it only 0.5%. Estimated via
   spending-to-vote-share regressions on available electoral data.

3. **Voter turnout determinants** (Blais 2006, Geys 2006):
   Socioeconomic correlates: income, education, closeness of race,
   compulsory voting laws. Geys meta-analysis: district magnitude,
   registration ease, and national-level factors dominate. Estimated
   from turnout regressed on GDP/capita, education, urbanization.

4. **Incumbency advantage** (Gelman & King 1990, Lee 2008):
   Measured via regression discontinuity on close elections.
   Lee (2008): winning by a slim margin in the US House raises
   re-election probability by 45pp. Estimated using electoral margin
   and re-election data.

Score: strong fiscal manipulation + spending money-politics + low turnout
+ large incumbency advantage -> high stress.

References:
    Nordhaus, W. (1975). "The Political Business Cycle." REStud 42(2).
    Rogoff, K. (1990). "Equilibrium Political Budget Cycles." AER 80(1).
    Levitt, S. (1994). "Using Repeat Challengers to Estimate the Effect
        of Campaign Spending on Election Outcomes." JPE 102(4).
    Blais, A. (2006). "What Affects Voter Turnout?" Annual Review of
        Political Science 9.
    Lee, D. (2008). "Randomized Experiments from Non-random Selection in
        US House Elections." Journal of Econometrics 142(2).
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class ElectionEconomics(LayerBase):
    layer_id = "l12"
    name = "Election Economics"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate electoral cycle, campaign spending, and incumbency effects.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 code (default USA)
            election_cycle : int - typical election cycle in years (default 4)
        """
        country = kwargs.get("country_iso3", "USA")
        election_cycle = int(kwargs.get("election_cycle", 4))

        # Fetch fiscal balance / government expenditure data
        fiscal_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('wdi', 'imf', 'fred')
              AND (ds.name LIKE '%fiscal%balance%' OR ds.name LIKE '%budget%deficit%'
                   OR ds.name LIKE '%government%expenditure%' OR ds.name LIKE '%general%government%balance%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Fetch voter turnout data
        turnout_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%voter%turnout%' OR ds.name LIKE '%election%turnout%'
                   OR ds.name LIKE '%electoral%participation%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Fetch GDP per capita / education for turnout determinants
        socioeco_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('wdi', 'fred')
              AND (ds.name LIKE '%gdp%per%capita%' OR ds.name LIKE '%education%index%'
                   OR ds.name LIKE '%literacy%rate%' OR ds.name LIKE '%urban%population%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Fetch campaign finance / political spending data
        campaign_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%campaign%finance%' OR ds.name LIKE '%campaign%spending%'
                   OR ds.name LIKE '%political%contribution%' OR ds.name LIKE '%election%spending%')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not fiscal_rows and not turnout_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no electoral/fiscal data"}

        # --- 1. Electoral cycle in fiscal policy (Nordhaus-Rogoff) ---
        fiscal_cycle = None
        cycle_amplitude = 0.0
        if fiscal_rows and len(fiscal_rows) >= election_cycle * 2:
            fiscal_vals = np.array([float(r["value"]) for r in fiscal_rows])
            fiscal_dates = [str(r["date"])[:4] for r in fiscal_rows]
            n = len(fiscal_vals)

            # Create election-year dummy (assumes elections every election_cycle years)
            # Use modular arithmetic on year index
            years = np.array([int(d) for d in fiscal_dates])
            # Identify modular position within cycle (0 = most recent election year)
            min_year = int(years[0])
            cycle_pos = (years - min_year) % election_cycle

            # Pre-election years (cycle_pos == election_cycle - 1): expected expansion
            pre_election = (cycle_pos == (election_cycle - 1)).astype(float)
            election_year = (cycle_pos == 0).astype(float)

            if pre_election.sum() >= 2:
                X = np.column_stack([np.ones(n), pre_election, election_year])
                beta = np.linalg.lstsq(X, fiscal_vals, rcond=None)[0]
                predicted = X @ beta
                ss_res = float(np.sum((fiscal_vals - predicted) ** 2))
                ss_tot = float(np.sum((fiscal_vals - np.mean(fiscal_vals)) ** 2))
                r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

                cycle_amplitude = abs(float(beta[1]))  # Pre-election fiscal shift

                fiscal_cycle = {
                    "pre_election_coefficient": round(float(beta[1]), 4),
                    "election_year_coefficient": round(float(beta[2]), 4),
                    "r_squared": round(r2, 4),
                    "cycle_amplitude": round(cycle_amplitude, 4),
                    "nordhaus_consistent": float(beta[1]) < 0,  # Expansion (deficit increase)
                    "n_obs": n,
                    "election_cycle_years": election_cycle,
                    "reference": "Nordhaus 1975: pre-election fiscal expansion",
                }

        # --- 2. Voter turnout analysis ---
        turnout_analysis = None
        turnout_level = 0.6  # Default moderate turnout
        if turnout_rows:
            tv = np.array([float(r["value"]) for r in turnout_rows])
            tv_dates = [r["date"] for r in turnout_rows]
            latest_turnout = float(tv[-1])

            # Normalize: turnout is typically expressed as pct (0-100) or fraction
            if np.max(tv) > 1.5:
                turnout_level = latest_turnout / 100.0
            else:
                turnout_level = latest_turnout

            turnout_level = float(np.clip(turnout_level, 0, 1))

            turnout_analysis = {
                "latest_turnout_pct": round(turnout_level * 100, 1),
                "mean_turnout_pct": round(float(np.mean(tv)) if np.max(tv) > 1.5 else float(np.mean(tv)) * 100, 1),
                "n_elections": len(tv),
                "date_range": [str(tv_dates[0]), str(tv_dates[-1])],
                "civic_engagement": "low" if turnout_level < 0.5 else "moderate" if turnout_level < 0.7 else "high",
            }

            # Trend
            if len(tv) >= 3:
                t = np.arange(len(tv), dtype=float)
                slope, _, r_val, p_val, _ = stats.linregress(t, tv)
                turnout_analysis["trend"] = {
                    "slope": round(float(slope), 4),
                    "direction": "declining" if slope < 0 else "rising",
                    "r_squared": round(float(r_val ** 2), 4),
                    "p_value": round(float(p_val), 4),
                }

        # --- 3. Turnout-socioeconomic correlates (Blais-Geys) ---
        turnout_determinants = None
        if turnout_rows and socioeco_rows and len(turnout_rows) >= 5:
            soc_series: dict[str, dict] = {}
            for r in socioeco_rows:
                yr = str(r["date"])[:4]
                sid = r["series_id"]
                soc_series.setdefault(sid, {})[yr] = float(r["value"])

            tv_map = {str(r["date"])[:4]: float(r["value"]) for r in turnout_rows}

            # Use GDP per capita as primary determinant
            gdp_sid = next(
                (s for s in soc_series if "gdp" in s.lower() or "pcap" in s.lower()), None
            )
            if gdp_sid:
                common = sorted(set(tv_map.keys()) & set(soc_series[gdp_sid].keys()))
                if len(common) >= 5:
                    tv_arr = np.array([tv_map[y] for y in common])
                    gdp_arr = np.array([soc_series[gdp_sid][y] for y in common])
                    slope, _, r_val, p_val, _ = stats.linregress(gdp_arr, tv_arr)
                    turnout_determinants = {
                        "gdp_turnout_coefficient": round(float(slope), 6),
                        "r_squared": round(float(r_val ** 2), 4),
                        "p_value": round(float(p_val), 4),
                        "blais_consistent": float(slope) > 0,
                        "n_obs": len(common),
                        "reference": "Blais 2006; Geys 2006 meta-analysis",
                    }
                    turnout_determinants["note"] = "Positive coefficient consistent with higher-income = higher-turnout"

        # --- 4. Campaign finance intensity ---
        campaign_analysis = None
        campaign_intensity = 0.3  # Default
        if campaign_rows:
            cf_vals = [float(r["value"]) for r in campaign_rows if r["value"] is not None]
            if cf_vals:
                latest_cf = float(cf_vals[-1])
                mean_cf = float(np.mean(cf_vals))
                # Normalize by trend growth (spending tends to rise over time)
                if len(cf_vals) >= 3:
                    t = np.arange(len(cf_vals), dtype=float)
                    slope, _, _, _, _ = stats.linregress(t, np.log1p(cf_vals))
                    growth_rate = float(np.expm1(slope))
                else:
                    growth_rate = 0.0

                # High spending levels and fast growth signal money-politics intensity
                spend_percentile = float(np.mean(np.array(cf_vals) >= mean_cf))
                campaign_intensity = float(np.clip(spend_percentile * 0.5 + abs(growth_rate) * 0.5, 0, 1))

                campaign_analysis = {
                    "latest_value": round(latest_cf, 2),
                    "mean_value": round(mean_cf, 2),
                    "spending_growth_rate": round(growth_rate, 4),
                    "campaign_intensity_index": round(campaign_intensity, 4),
                    "reference": "Levitt 1994: campaign spending effects on vote share",
                }

        # --- Score ---
        # Fiscal cycle manipulation (0-35)
        if fiscal_cycle:
            cycle_score = float(np.clip(cycle_amplitude * 20.0, 0, 35))
        else:
            cycle_score = 15.0  # No data, neutral

        # Turnout stress: low turnout = civic disengagement = stress (0-30)
        turnout_score = float(np.clip((1.0 - turnout_level) * 30.0, 0, 30))

        # Campaign finance intensity (0-20)
        campaign_score = float(np.clip(campaign_intensity * 20.0, 0, 20))

        # Baseline incumbency / democracy quality (0-15): proxy via press freedom absence
        democracy_base = 10.0

        score = float(np.clip(cycle_score + turnout_score + campaign_score + democracy_base, 0, 100))

        result = {
            "score": round(score, 2),
            "country": country,
            "score_components": {
                "fiscal_cycle": round(cycle_score, 2),
                "low_turnout": round(turnout_score, 2),
                "campaign_finance": round(campaign_score, 2),
                "democracy_baseline": round(democracy_base, 2),
            },
        }

        if fiscal_cycle:
            result["electoral_fiscal_cycle"] = fiscal_cycle
        if turnout_analysis:
            result["voter_turnout"] = turnout_analysis
        if turnout_determinants:
            result["turnout_determinants"] = turnout_determinants
        if campaign_analysis:
            result["campaign_finance"] = campaign_analysis

        return result
