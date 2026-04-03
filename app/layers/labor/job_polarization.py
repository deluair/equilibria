"""Job polarization: Autor-Dorn routine task intensity and hollowing out.

The Autor-Dorn (2013) framework explains why employment grows at the tails
(high-skill abstract tasks, low-skill manual tasks) while middle-skill
routine-cognitive and routine-manual jobs decline.

Four analytical components:

1. Routine Task Intensity (RTI) index by occupation:
   RTI_k = ln(T_R_k) - ln(T_M_k) - ln(T_A_k)
   where T_R = routine task intensity, T_M = manual intensity,
   T_A = abstract intensity (Autor, Levy & Murnane 2003).
   High RTI occupations are most susceptible to computerization.

2. Hollowing out test: employment share change in middle-skill tercile
   vs top and bottom terciles. Goos-Manning-Salomons (2014) identify
   U-shaped employment growth across the wage distribution in Europe.

3. Wage polarization: 90/50 and 50/10 percentile log wage ratios over
   time. Autor-Katz-Kearney (2008): top inequality (90/50) driven by
   SBTC; bottom inequality (50/10) driven by routinization + minimum wage.

4. Employment share shifts by skill tier: annual percentage point change
   in employment shares for low-, middle-, and high-skill occupations.
   Convergence to U-shaped distribution confirms polarization.

References:
    Autor, D. & Dorn, D. (2013). The growth of low-skill service jobs and
        the polarization of the US labor market. AER 103(5): 1553-1597.
    Goos, M., Manning, A. & Salomons, A. (2014). Explaining job polarization:
        Routine-biased technological change and offshoring.
        AER 104(8): 2509-2526.
    Autor, D., Katz, L. & Kearney, M. (2008). Trends in U.S. wage inequality:
        Revising the revisionists. ReStat 90(2): 300-323.
    Autor, D., Levy, F. & Murnane, R. (2003). The skill content of recent
        technological change. QJE 118(4): 1279-1333.

Score: pronounced U-shape with middle-skill hollowing + wage polarization
-> STRESS. Upgrading (all growth at top) -> WATCH. No polarization -> STABLE.
"""

import json

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class JobPolarization(LayerBase):
    layer_id = "l3"
    name = "Job Polarization (Autor-Dorn)"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata, ds.description
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'job_polarization'
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        if not rows or len(rows) < 6:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient polarization data"}

        # Parse observations by indicator type
        tier_shares: dict[str, list[tuple[str, float]]] = {
            "high": [], "middle": [], "low": []
        }
        rti_values: list[tuple[str, float, str]] = []  # (date, rti, occupation)
        wage_p90: list[tuple[str, float]] = []
        wage_p50: list[tuple[str, float]] = []
        wage_p10: list[tuple[str, float]] = []

        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            indicator = meta.get("indicator", row.get("description", ""))
            val = row["value"]
            date = row["date"]
            if val is None:
                continue

            if "high_skill_share" in indicator or "tier_high" in indicator:
                tier_shares["high"].append((date, float(val)))
            elif "middle_skill_share" in indicator or "tier_middle" in indicator:
                tier_shares["middle"].append((date, float(val)))
            elif "low_skill_share" in indicator or "tier_low" in indicator:
                tier_shares["low"].append((date, float(val)))
            elif "rti_index" in indicator or "routine_task_intensity" in indicator:
                occ = meta.get("occupation", "aggregate")
                rti_values.append((date, float(val), occ))
            elif "wage_p90" in indicator:
                wage_p90.append((date, float(val)))
            elif "wage_p50" in indicator or "wage_median" in indicator:
                wage_p50.append((date, float(val)))
            elif "wage_p10" in indicator:
                wage_p10.append((date, float(val)))

        # Hollowing out analysis
        hollowing_result = None
        has_tier_data = any(len(v) >= 3 for v in tier_shares.values())
        if has_tier_data:
            # Compute share changes over available period for each tier
            tier_changes = {}
            for tier, series in tier_shares.items():
                if len(series) >= 2:
                    series_sorted = sorted(series, key=lambda x: x[0])
                    delta = series_sorted[-1][1] - series_sorted[0][1]
                    tier_changes[tier] = round(delta, 4)

            if len(tier_changes) >= 2:
                middle_change = tier_changes.get("middle", None)
                high_change = tier_changes.get("high", None)
                low_change = tier_changes.get("low", None)

                # U-shape: middle declining, tails growing
                is_u_shaped = (
                    middle_change is not None and middle_change < -0.01
                    and (
                        (high_change is not None and high_change > 0)
                        or (low_change is not None and low_change > 0)
                    )
                )
                # Upgrading: only high grows, middle and low decline
                is_upgrading = (
                    high_change is not None and high_change > 0.01
                    and middle_change is not None and middle_change < 0
                    and (low_change is None or low_change <= 0)
                )

                hollowing_result = {
                    "share_changes": tier_changes,
                    "is_polarizing": is_u_shaped,
                    "is_upgrading": is_upgrading,
                    "pattern": (
                        "U-shaped polarization" if is_u_shaped
                        else "skill upgrading" if is_upgrading
                        else "mixed/no clear pattern"
                    ),
                }

        # RTI analysis
        rti_result = None
        if rti_values:
            recent_rtis = [(v, occ) for (d, v, occ) in rti_values]
            rti_arr = np.array([v for v, _ in recent_rtis])
            avg_rti = float(np.mean(rti_arr))
            pct_high_rti = float(np.mean(rti_arr > 0.5)) * 100.0  # share with high routine content

            rti_result = {
                "mean_rti": round(avg_rti, 4),
                "pct_high_routine": round(pct_high_rti, 2),
                "n_occupations": len(rti_values),
                "interpretation": (
                    "high routine task concentration, vulnerable to computerization"
                    if avg_rti > 0.3
                    else "moderate routine content"
                    if avg_rti > 0
                    else "abstract/manual dominant"
                ),
            }

        # Wage polarization analysis
        wage_result = None
        if len(wage_p90) >= 3 and len(wage_p50) >= 3:
            p90_sorted = sorted(wage_p90, key=lambda x: x[0])
            p50_sorted = sorted(wage_p50, key=lambda x: x[0])

            p90_vals = np.array([v for _, v in p90_sorted])
            p50_vals = np.array([v for _, v in p50_sorted])

            # 90/50 ratio trend
            ratio_9050 = p90_vals / p50_vals
            t_idx = np.arange(len(ratio_9050), dtype=float)
            slope_9050, _, r_9050, _, _ = sp_stats.linregress(t_idx, ratio_9050)

            wage_result = {
                "current_ratio_9050": round(float(ratio_9050[-1]), 4),
                "trend_9050_per_period": round(float(slope_9050), 4),
                "r_squared_9050": round(float(r_9050 ** 2), 4),
                "top_inequality_rising": float(slope_9050) > 0.001,
            }

            if len(wage_p10) >= 3:
                p10_sorted = sorted(wage_p10, key=lambda x: x[0])
                p10_vals = np.array([v for _, v in p10_sorted])
                n_joint = min(len(p50_vals), len(p10_vals))
                ratio_5010 = p50_vals[:n_joint] / p10_vals[:n_joint]
                t2 = np.arange(n_joint, dtype=float)
                slope_5010, _, r_5010, _, _ = sp_stats.linregress(t2, ratio_5010)
                wage_result["current_ratio_5010"] = round(float(ratio_5010[-1]), 4)
                wage_result["trend_5010_per_period"] = round(float(slope_5010), 4)
                wage_result["bottom_inequality_rising"] = float(slope_5010) > 0.001

        # Score: polarization stress
        score = 25.0  # baseline

        if hollowing_result:
            if hollowing_result["is_polarizing"]:
                mid_chg = hollowing_result["share_changes"].get("middle", 0)
                score += min(35.0, abs(mid_chg) * 2000.0)
            elif hollowing_result["is_upgrading"]:
                score += 10.0  # some adjustment cost but not polarization

        if rti_result:
            score += min(20.0, rti_result["pct_high_routine"] * 0.5)

        if wage_result:
            if wage_result.get("top_inequality_rising"):
                score += 10.0
            if wage_result.get("bottom_inequality_rising"):
                score += 10.0

        score = max(0.0, min(100.0, score))

        result = {
            "score": round(score, 2),
            "country": country,
        }

        if hollowing_result:
            result["hollowing_out"] = hollowing_result
        if rti_result:
            result["routine_task_intensity"] = rti_result
        if wage_result:
            result["wage_polarization"] = wage_result

        return result
