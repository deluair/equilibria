"""Labor market tightness: V/U ratio and matching function estimation.

Labor market tightness theta = V/U (vacancy rate / unemployment rate) is a
key state variable in the Diamond-Mortensen-Pissarides (DMP) search and
matching framework. It summarizes the balance of power between firms and
workers.

Matching function (Pissarides 2000):
    M(u, v) = A * u^alpha * v^(1-alpha)

where M is the flow of new hires, A is matching efficiency, and alpha is the
elasticity of matches with respect to unemployment (typically 0.5-0.7).

In intensive form:
    m = M/u = A * theta^(1-alpha)

Estimation:
    ln(M_t/u_t) = ln(A) + (1-alpha)*ln(V_t/u_t) + e_t

Tightness regimes:
    theta > 1: tight market (more vacancies than unemployed). Firms struggle
               to hire. Wage pressure upward.
    theta < 0.5: slack market. Workers compete for scarce jobs.
    theta ~ 0.7: balanced (US historical average)

Time-varying matching efficiency:
    A_t can decline due to mismatch, extended UI, pandemic disruptions.
    Estimated as residual from matching function.

References:
    Pissarides, C. (2000). Equilibrium Unemployment Theory. MIT Press.
    Diamond, P. (1982). Aggregate Demand Management in Search Equilibrium.
        Journal of Political Economy 90(5): 881-894.
    Barnichon, R. & Figura, A. (2015). Labor Market Heterogeneity and the
        Aggregate Matching Function. AEJ: Macro 7(4): 222-249.

Score: very tight (theta > 1.5) or very slack (theta < 0.3) -> STRESS.
Balanced tightness -> STABLE.
"""

import numpy as np
from app.layers.base import LayerBase


class LaborMarketTightness(LayerBase):
    layer_id = "l3"
    name = "Labor Market Tightness"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'labor_tightness'
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient tightness data"}

        import json

        dates = []
        vacancies = []
        unemployment = []
        hires = []

        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            v = row["value"]
            u = meta.get("unemployment")
            h = meta.get("hires")
            if v is None or u is None:
                continue
            v, u = float(v), float(u)
            if v <= 0 or u <= 0:
                continue
            dates.append(row["date"])
            vacancies.append(v)
            unemployment.append(u)
            hires.append(float(h) if h is not None else None)

        n = len(vacancies)
        if n < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid obs"}

        v_arr = np.array(vacancies)
        u_arr = np.array(unemployment)
        theta = v_arr / u_arr  # tightness

        current_theta = float(theta[-1])
        mean_theta = float(np.mean(theta))
        theta_trend = float(np.polyfit(range(n), theta, 1)[0])

        # Matching function estimation if hires data available
        matching_result = None
        valid_hires = [(h, v, u) for h, v, u in zip(hires, vacancies, unemployment)
                       if h is not None and h > 0]
        if len(valid_hires) >= 10:
            h_arr = np.array([x[0] for x in valid_hires])
            v_m = np.array([x[1] for x in valid_hires])
            u_m = np.array([x[2] for x in valid_hires])

            # ln(M) = ln(A) + alpha*ln(u) + (1-alpha)*ln(v)
            X_match = np.column_stack([np.ones(len(h_arr)), np.log(u_m), np.log(v_m)])
            y_match = np.log(h_arr)
            beta_match = np.linalg.lstsq(X_match, y_match, rcond=None)[0]

            matching_efficiency = float(np.exp(beta_match[0]))
            alpha = float(beta_match[1])  # elasticity wrt unemployment
            one_minus_alpha = float(beta_match[2])  # elasticity wrt vacancies

            resid_match = y_match - X_match @ beta_match
            ss_res = np.sum(resid_match ** 2)
            ss_tot = np.sum((y_match - y_match.mean()) ** 2)
            r2_match = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

            # Time-varying efficiency (residuals from constant-A model)
            efficiency_residuals = np.exp(resid_match).tolist()

            # CRS test: alpha + (1-alpha) should = 1
            crs_sum = alpha + one_minus_alpha

            matching_result = {
                "matching_efficiency_A": round(matching_efficiency, 4),
                "alpha_unemployment": round(alpha, 4),
                "one_minus_alpha_vacancy": round(one_minus_alpha, 4),
                "returns_to_scale": round(crs_sum, 4),
                "constant_returns": abs(crs_sum - 1.0) < 0.1,
                "r_squared": round(r2_match, 4),
            }

        # Beveridge curve position (current vs historical)
        if n >= 10:
            hist_mean_u = float(np.mean(u_arr[:-4]))
            hist_mean_v = float(np.mean(v_arr[:-4]))
            recent_mean_u = float(np.mean(u_arr[-4:]))
            recent_mean_v = float(np.mean(v_arr[-4:]))
            beveridge_shift = (recent_mean_v / recent_mean_u) - (hist_mean_v / hist_mean_u)
        else:
            beveridge_shift = 0.0

        # Score: extreme tightness or slackness -> STRESS
        if current_theta > 1.5:
            score = 50.0 + (current_theta - 1.5) * 40.0  # overheating
        elif current_theta > 1.0:
            score = 30.0 + (current_theta - 1.0) * 40.0
        elif current_theta < 0.3:
            score = 60.0 + (0.3 - current_theta) * 100.0  # severe slack
        elif current_theta < 0.5:
            score = 35.0 + (0.5 - current_theta) * 125.0
        else:
            # Balanced range 0.5-1.0
            score = 10.0 + abs(current_theta - 0.7) * 60.0
        score = max(0.0, min(100.0, score))

        result = {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "tightness": {
                "current_vu_ratio": round(current_theta, 3),
                "mean_vu_ratio": round(mean_theta, 3),
                "trend_per_period": round(theta_trend, 4),
                "regime": (
                    "very tight" if current_theta > 1.5
                    else "tight" if current_theta > 1.0
                    else "balanced" if current_theta > 0.5
                    else "slack" if current_theta > 0.3
                    else "very slack"
                ),
            },
            "current": {
                "vacancies": round(float(v_arr[-1]), 0),
                "unemployment": round(float(u_arr[-1]), 0),
            },
            "beveridge_shift": round(beveridge_shift, 4),
            "time_range": {
                "start": dates[0] if dates else None,
                "end": dates[-1] if dates else None,
            },
        }

        if matching_result:
            result["matching_function"] = matching_result

        return result
