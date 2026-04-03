"""Labor force participation rate analysis and decomposition.

Tracks LFPR trends by demographic group and decomposes aggregate changes into
within-group (behavioral) and between-group (compositional) components.

Decomposition (Aaronson et al. 2006):
    dLFPR = sum_g [share_g * d(lfpr_g)] + sum_g [lfpr_g * d(share_g)]
          = Within (behavioral)         + Between (compositional)

The within component captures genuine changes in participation behavior
(e.g. women entering workforce, older workers retiring later). The between
component captures demographic shifts (e.g. aging population reducing
aggregate LFPR because older groups have lower participation).

Discouraged worker effect:
    Workers who stop searching (exit labor force) during downturns are not
    counted as unemployed. The U-3 rate understates true labor market slack.
    U-6 (broad unemployment) or the employment-population ratio are better
    indicators.

    Discouraged = f(unemployment_rate, duration, benefits)

Key patterns:
    - US prime-age male LFPR: declining since 1960s (Krueger 2017)
    - Female LFPR: rose 1960-2000, plateaued (Goldin 2006)
    - 55+: rising due to improved health, pension changes
    - BD: low female LFPR (~36%), rising slowly

References:
    Aaronson, S., Fallick, B., Figura, A., Pingle, J. & Wascher, W. (2006).
        The Recent Decline in the Labor Force Participation Rate and Its
        Implications for Potential Labor Supply. Brookings Papers.
    Krueger, A. (2017). Where Have All the Workers Gone? An Inquiry into
        the Decline of the U.S. Labor Force Participation Rate. Brookings.
    Goldin, C. (2006). The Quiet Revolution That Transformed Women's
        Employment, Education, and Family. AER P&P 96(2).

Score: declining aggregate LFPR with large discouraged worker share -> STRESS.
Rising or stable LFPR -> STABLE.
"""

import numpy as np

from app.layers.base import LayerBase


class LaborForceParticipation(LayerBase):
    layer_id = "l3"
    name = "Labor Force Participation"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata, ds.description
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'labor_force'
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient LFPR data"}

        import json

        dates = []
        aggregate_lfpr = []
        group_data = {}  # {group: [(date, lfpr, pop_share)]}

        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            lfpr = row["value"]
            if lfpr is None:
                continue
            lfpr = float(lfpr)
            date = row["date"]
            group = meta.get("demographic_group", "aggregate")
            pop_share = meta.get("population_share")

            if group == "aggregate":
                dates.append(date)
                aggregate_lfpr.append(lfpr)
            else:
                if group not in group_data:
                    group_data[group] = []
                group_data[group].append({
                    "date": date,
                    "lfpr": lfpr,
                    "pop_share": float(pop_share) if pop_share is not None else None,
                })

        n = len(aggregate_lfpr)
        if n < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient aggregate obs"}

        lfpr_arr = np.array(aggregate_lfpr)

        # Trend: OLS on time index
        t_idx = np.arange(n, dtype=float)
        X_trend = np.column_stack([np.ones(n), t_idx])
        beta_trend = np.linalg.lstsq(X_trend, lfpr_arr, rcond=None)[0]
        trend_slope = float(beta_trend[1])  # pp change per period

        # Recent change
        if n >= 4:
            recent_change = float(lfpr_arr[-1] - lfpr_arr[-4])
        else:
            recent_change = float(lfpr_arr[-1] - lfpr_arr[0])

        # Decomposition if group data available
        decomposition = None
        if group_data and len(group_data) >= 2:
            # Need at least 2 periods with group data
            within_total = 0.0
            between_total = 0.0
            group_results = {}

            for grp, obs_list in group_data.items():
                if len(obs_list) < 2:
                    continue
                obs_list.sort(key=lambda x: x["date"])
                first = obs_list[0]
                last = obs_list[-1]
                if first["pop_share"] is None or last["pop_share"] is None:
                    continue

                d_lfpr = last["lfpr"] - first["lfpr"]
                d_share = last["pop_share"] - first["pop_share"]
                avg_share = (first["pop_share"] + last["pop_share"]) / 2.0
                avg_lfpr = (first["lfpr"] + last["lfpr"]) / 2.0

                within_contrib = avg_share * d_lfpr
                between_contrib = avg_lfpr * d_share

                within_total += within_contrib
                between_total += between_contrib

                group_results[grp] = {
                    "lfpr_change": round(d_lfpr, 2),
                    "share_change": round(d_share, 4),
                    "within_contribution": round(within_contrib, 4),
                    "between_contribution": round(between_contrib, 4),
                }

            if group_results:
                decomposition = {
                    "within_behavioral": round(within_total, 4),
                    "between_compositional": round(between_total, 4),
                    "total": round(within_total + between_total, 4),
                    "groups": group_results,
                }

        # Current level assessment
        current_lfpr = float(lfpr_arr[-1])

        # Score: declining LFPR -> STRESS, low level -> WATCH
        if trend_slope < -0.3:
            score = 60.0 + abs(trend_slope) * 30.0
        elif trend_slope < 0:
            score = 30.0 + abs(trend_slope) * 100.0
        elif current_lfpr < 50:
            score = 40.0 + (50 - current_lfpr) * 1.0
        else:
            score = max(5.0, 30.0 - trend_slope * 20.0)
        score = max(0.0, min(100.0, score))

        result = {
            "score": round(score, 2),
            "country": country,
            "n_periods": n,
            "current_lfpr": round(current_lfpr, 2),
            "trend": {
                "slope_pp_per_period": round(trend_slope, 3),
                "direction": "rising" if trend_slope > 0.05 else "declining" if trend_slope < -0.05 else "stable",
                "recent_change_pp": round(recent_change, 2),
            },
            "time_range": {
                "start": dates[0] if dates else None,
                "end": dates[-1] if dates else None,
            },
        }

        if decomposition:
            result["decomposition"] = decomposition

        return result
