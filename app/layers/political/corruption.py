"""Corruption-growth nexus and rent-seeking cost estimation.

Mauro (1995) corruption-growth nexus: cross-country regressions show a one
standard deviation increase in corruption reduces investment by 2.9% of GDP
and growth by 0.5 pp/year. Corruption acts as a tax on investment.

Two competing hypotheses:
    "Grease the wheels" (Leff 1964, Huntington 1968): corruption accelerates
    transactions in bureaucratically rigid economies. Predicts positive
    growth effect when regulation is excessive.

    "Sand the wheels" (Mauro 1995, Shleifer-Vishny 1993): corruption distorts
    allocation, increases uncertainty, and deters investment. Predicts
    negative growth effect unconditionally.

Empirically, sand dominates: Meon & Sekkat (2005) find corruption hurts
growth even in highly regulated economies.

Tullock (1967) rent-seeking: resources spent competing for rents
(monopolies, licenses) are socially wasteful. If R is the value of the rent
and N agents compete, the rent-seeking cost is:

    RSC = sum(x_i) where x_i are individual expenditures

In equilibrium with identical agents: x_i = R * (N-1) / N^2, total
RSC = R * (N-1) / N. The rent-seeking loss approaches R as N grows.

Score: high corruption + evidence of sand-the-wheels + large rent-seeking
costs -> high stress.

References:
    Mauro, P. (1995). "Corruption and Growth." QJE 110(3).
    Shleifer, A. & Vishny, R. (1993). "Corruption." QJE 108(3).
    Meon, P.-G. & Sekkat, K. (2005). "Does Corruption Grease or Sand the
        Wheels of Growth?" Public Choice 122.
    Tullock, G. (1967). "The Welfare Costs of Tariffs, Monopolies, and
        Theft." Western Economic Journal 5(3).
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class CorruptionEconomics(LayerBase):
    layer_id = "l12"
    name = "Corruption Economics"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate corruption-growth nexus and rent-seeking costs.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 code (default BGD)
            n_rent_seekers : int - number of competing agents for Tullock model
        """
        country = kwargs.get("country_iso3", "BGD")
        n_rent_seekers = kwargs.get("n_rent_seekers", 10)

        # Fetch corruption perception index (CPI, WGI control of corruption, V-Dem)
        corruption_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name, ds.source
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%corruption%' OR ds.name LIKE '%control%corruption%'
                   OR ds.name LIKE '%bribery%' OR ds.name LIKE '%transparency%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Fetch GDP growth for nexus test
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

        # Fetch investment/GDP ratio
        invest_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('wdi', 'imf')
              AND (ds.name LIKE '%gross%capital%formation%' OR ds.name LIKE '%investment%gdp%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Fetch regulation quality (for grease vs sand test)
        regulation_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%regulatory%quality%' OR ds.name LIKE '%ease%business%'
                   OR ds.name LIKE '%bureaucratic%quality%')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not corruption_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no corruption data"}

        # Parse corruption series
        corruption_dates = [r["date"] for r in corruption_rows]
        corruption_vals = np.array([float(r["value"]) for r in corruption_rows])
        corruption_years = np.array([int(str(d)[:4]) for d in corruption_dates])

        latest_corruption = float(corruption_vals[-1])
        mean_corruption = float(np.mean(corruption_vals))

        # Corruption trend
        if len(corruption_vals) >= 3:
            t = np.arange(len(corruption_vals), dtype=float)
            slope, intercept, r_val, p_val, se = stats.linregress(t, corruption_vals)
            corruption_trend = {
                "slope": round(slope, 4),
                "direction": "worsening" if slope < 0 else "improving",
                "r_squared": round(r_val ** 2, 4),
                "p_value": round(p_val, 4),
            }
        else:
            corruption_trend = None

        # --- Mauro corruption-growth nexus ---
        nexus_result = None
        if growth_rows and len(growth_rows) >= 5:
            g_dates = {str(r["date"])[:4]: float(r["value"]) for r in growth_rows}
            c_dates = {str(d)[:4]: float(v) for d, v in zip(corruption_dates, corruption_vals)}
            common = sorted(set(g_dates.keys()) & set(c_dates.keys()))

            if len(common) >= 5:
                g_arr = np.array([g_dates[y] for y in common])
                c_arr = np.array([c_dates[y] for y in common])

                # OLS: growth = b0 + b1*corruption + e
                slope, intercept, r_val, p_val, se = stats.linregress(c_arr, g_arr)
                nexus_result = {
                    "corruption_growth_coefficient": round(slope, 4),
                    "p_value": round(p_val, 4),
                    "r_squared": round(r_val ** 2, 4),
                    "effect_direction": "positive" if slope > 0 else "negative",
                    "mauro_consistent": slope < 0,  # Sand the wheels
                    "n_observations": len(common),
                }

        # --- Corruption-investment channel ---
        investment_channel = None
        if invest_rows and len(invest_rows) >= 5:
            inv_dates = {str(r["date"])[:4]: float(r["value"]) for r in invest_rows}
            c_dates = {str(d)[:4]: float(v) for d, v in zip(corruption_dates, corruption_vals)}
            common = sorted(set(inv_dates.keys()) & set(c_dates.keys()))

            if len(common) >= 5:
                inv_arr = np.array([inv_dates[y] for y in common])
                c_arr = np.array([c_dates[y] for y in common])
                slope, intercept, r_val, p_val, se = stats.linregress(c_arr, inv_arr)
                investment_channel = {
                    "corruption_investment_coefficient": round(slope, 4),
                    "p_value": round(p_val, 4),
                    "r_squared": round(r_val ** 2, 4),
                    "investment_depressed": slope < 0,
                    "n_observations": len(common),
                }

        # --- Grease vs sand the wheels ---
        grease_vs_sand = None
        if regulation_rows and nexus_result:
            reg_dates = {str(r["date"])[:4]: float(r["value"]) for r in regulation_rows}
            c_dates = {str(d)[:4]: float(v) for d, v in zip(corruption_dates, corruption_vals)}
            g_dates_map = {str(r["date"])[:4]: float(r["value"]) for r in growth_rows}
            common = sorted(set(reg_dates.keys()) & set(c_dates.keys()) & set(g_dates_map.keys()))

            if len(common) >= 5:
                g_arr = np.array([g_dates_map[y] for y in common])
                c_arr = np.array([c_dates[y] for y in common])
                reg_arr = np.array([reg_dates[y] for y in common])

                # Interaction: corruption * regulation
                interaction = c_arr * reg_arr
                X = np.column_stack([np.ones(len(g_arr)), c_arr, reg_arr, interaction])
                beta = np.linalg.lstsq(X, g_arr, rcond=None)[0]

                grease_vs_sand = {
                    "corruption_coefficient": round(float(beta[1]), 4),
                    "regulation_coefficient": round(float(beta[2]), 4),
                    "interaction_coefficient": round(float(beta[3]), 4),
                    "hypothesis": "grease" if beta[3] > 0 else "sand",
                    "note": "Positive interaction: corruption mitigates poor regulation (grease). "
                            "Negative: corruption compounds poor regulation (sand).",
                    "n_observations": len(common),
                }

        # --- Tullock rent-seeking cost ---
        # Model: N symmetric agents compete for rent R
        # Nash equilibrium expenditure: x_i = R*(N-1)/N^2
        # Total dissipation: RSC = R*(N-1)/N
        # Dissipation rate: (N-1)/N
        N = max(n_rent_seekers, 2)
        dissipation_rate = (N - 1) / N
        per_agent_share = (N - 1) / (N ** 2)

        # Estimate rent value from corruption-related GDP loss
        # Mauro: 1 SD corruption reduction -> 0.5pp growth. Use corruption level as proxy.
        # Rough: rent-seeking cost as % of GDP proportional to corruption
        if mean_corruption > 0:
            # Normalize: assume corruption index 0-100 (CPI style) or -2.5 to 2.5 (WGI style)
            # Detect scale
            if np.min(corruption_vals) >= -3 and np.max(corruption_vals) <= 3:
                # WGI scale (-2.5 to 2.5); lower = more corrupt
                normalized = (2.5 - latest_corruption) / 5.0  # 0 = clean, 1 = corrupt
            elif np.max(corruption_vals) <= 100:
                # CPI scale (0-100); higher = cleaner
                normalized = (100 - latest_corruption) / 100.0
            else:
                normalized = 0.5  # Unknown scale

            estimated_rsc_pct_gdp = normalized * dissipation_rate * 5.0  # Up to 5% GDP
        else:
            normalized = 0.5
            estimated_rsc_pct_gdp = 2.5

        rent_seeking = {
            "n_agents": N,
            "dissipation_rate": round(dissipation_rate, 4),
            "per_agent_share": round(per_agent_share, 4),
            "estimated_rsc_pct_gdp": round(estimated_rsc_pct_gdp, 2),
            "note": f"Tullock model: {N} agents compete, {round(dissipation_rate*100,1)}% of rent dissipated",
        }

        # --- Score ---
        # Higher corruption -> higher score
        # Normalized corruption level (0 = clean, 1 = corrupt)
        corruption_component = normalized * 50.0

        # Growth nexus: sand the wheels confirmed
        nexus_component = 0.0
        if nexus_result:
            if nexus_result["mauro_consistent"] and nexus_result["p_value"] < 0.10:
                nexus_component = 25.0
            elif nexus_result["mauro_consistent"]:
                nexus_component = 15.0
            else:
                nexus_component = 5.0  # Grease or insignificant
        else:
            nexus_component = 12.5

        # Rent-seeking costs
        rsc_component = min(25.0, estimated_rsc_pct_gdp * 5.0)

        score = float(np.clip(corruption_component + nexus_component + rsc_component, 0, 100))

        result = {
            "score": round(score, 2),
            "country": country,
            "latest_corruption_index": round(latest_corruption, 2),
            "mean_corruption_index": round(mean_corruption, 2),
            "normalized_corruption": round(normalized, 4),
            "n_observations": len(corruption_vals),
            "date_range": [str(corruption_dates[0]), str(corruption_dates[-1])],
            "rent_seeking": rent_seeking,
        }

        if corruption_trend:
            result["trend"] = corruption_trend
        if nexus_result:
            result["mauro_nexus"] = nexus_result
        if investment_channel:
            result["investment_channel"] = investment_channel
        if grease_vs_sand:
            result["grease_vs_sand"] = grease_vs_sand

        return result
