"""College wage premium and skill-biased demand analysis.

The skill premium (college vs high school wages) reflects the race between
education (supply) and technology (demand) per Tinbergen (1974).

Katz-Murphy (1992) supply-demand framework:
    ln(w_c/w_h) = b0 + b1*(D_t/S_t) + e_t

where D_t is relative demand for college workers (driven by technology,
trade, institutions) and S_t is relative supply (college/HS workers).

The premium rose sharply in the US from 1980-2000 as SBTC outpaced
educational attainment growth. It has since stabilized/modestly risen.

Alternative decomposition (Autor, Katz & Kearney 2008):
    - Polarization: hollowing of middle-skill jobs
    - Top inequality (90/50) driven by returns to abstract tasks
    - Bottom inequality (50/10) driven by routinization

Key metrics:
    - Raw premium: mean(ln_wage_college) - mean(ln_wage_hs)
    - Composition-adjusted premium: controlling for experience, demographics
    - Relative supply index: college equiv / HS equiv workers

References:
    Tinbergen, J. (1974). Substitution of Graduate by Other Labour.
        Kyklos 27(2): 217-226.
    Katz, L. & Murphy, K. (1992). Changes in Relative Wages, 1963-1987:
        Supply and Demand Factors. QJE 107(1): 35-78.
    Goldin, C. & Katz, L. (2008). The Race Between Education and Technology.
        Harvard University Press.
    Autor, D., Katz, L. & Kearney, M. (2008). Trends in U.S. Wage
        Inequality: Revising the Revisionists. ReStat 90(2): 300-323.

Score: rapidly rising premium -> STRESS (inequality/mismatch). Stable or
moderately high premium -> WATCH. Declining premium -> potential devaluation
of education signal.
"""

import numpy as np
from app.layers.base import LayerBase


class SkillPremium(LayerBase):
    layer_id = "l3"
    name = "Skill Premium (College/HS)"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'skill_premium'
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient skill premium data"}

        import json

        dates = []
        premiums = []
        supply_ratios = []
        demand_indices = []

        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            premium = row["value"]
            if premium is None:
                continue
            dates.append(row["date"])
            premiums.append(float(premium))
            sr = meta.get("relative_supply")
            di = meta.get("demand_index")
            supply_ratios.append(float(sr) if sr is not None else None)
            demand_indices.append(float(di) if di is not None else None)

        n = len(premiums)
        if n < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid obs"}

        prem_arr = np.array(premiums)

        # Trend in premium
        t_idx = np.arange(n, dtype=float)
        X_t = np.column_stack([np.ones(n), t_idx])
        beta_t = np.linalg.lstsq(X_t, prem_arr, rcond=None)[0]
        trend_slope = float(beta_t[1])

        current_premium = float(prem_arr[-1])
        mean_premium = float(np.mean(prem_arr))

        # Katz-Murphy estimation if supply data available
        katz_murphy = None
        valid_supply = [(s, p) for s, p in zip(supply_ratios, premiums) if s is not None and s > 0]
        if len(valid_supply) >= 5:
            sr_arr = np.array([x[0] for x in valid_supply])
            pr_arr = np.array([x[1] for x in valid_supply])
            ln_sr = np.log(sr_arr)
            X_km = np.column_stack([np.ones(len(ln_sr)), ln_sr])
            beta_km = np.linalg.lstsq(X_km, pr_arr, rcond=None)[0]
            resid_km = pr_arr - X_km @ beta_km
            ss_res = np.sum(resid_km ** 2)
            ss_tot = np.sum((pr_arr - pr_arr.mean()) ** 2)
            r2_km = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

            # Elasticity of substitution sigma = -1/beta_km[1]
            sigma = -1.0 / beta_km[1] if abs(beta_km[1]) > 1e-6 else float("inf")

            katz_murphy = {
                "supply_elasticity": round(float(beta_km[1]), 4),
                "elasticity_of_substitution": round(sigma, 2) if abs(sigma) < 100 else None,
                "r_squared": round(r2_km, 4),
                "interpretation": (
                    "supply increase reduces premium" if beta_km[1] < 0
                    else "anomalous: supply increase raises premium"
                ),
            }

        # Acceleration: is premium growth accelerating or decelerating?
        if n >= 6:
            mid = n // 2
            early_slope = float(np.polyfit(range(mid), prem_arr[:mid], 1)[0])
            late_slope = float(np.polyfit(range(n - mid), prem_arr[mid:], 1)[0])
            acceleration = late_slope - early_slope
        else:
            acceleration = 0.0

        # Score: rising premium = inequality/mismatch -> STRESS
        if trend_slope > 0.02:
            score = 50.0 + trend_slope * 500.0
        elif trend_slope > 0:
            score = 25.0 + trend_slope * 1250.0
        elif current_premium > 0.8:  # very high level even if stable
            score = 40.0 + (current_premium - 0.8) * 50.0
        else:
            score = 15.0 + current_premium * 15.0
        score = max(0.0, min(100.0, score))

        result = {
            "score": round(score, 2),
            "country": country,
            "n_periods": n,
            "current_premium": round(current_premium, 4),
            "mean_premium": round(mean_premium, 4),
            "trend": {
                "slope_per_period": round(trend_slope, 4),
                "direction": "rising" if trend_slope > 0.005 else "declining" if trend_slope < -0.005 else "stable",
                "acceleration": round(acceleration, 4),
            },
            "time_range": {
                "start": dates[0] if dates else None,
                "end": dates[-1] if dates else None,
            },
        }

        if katz_murphy:
            result["katz_murphy"] = katz_murphy

        return result
