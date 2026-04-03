"""Taylor Rule estimation module.

Methodology
-----------
The Taylor (1993) rule prescribes a nominal interest rate:

    i_t = r* + pi_t + phi_pi * (pi_t - pi*) + phi_y * y_t

where:
    i_t   = federal funds rate (or policy rate)
    r*    = equilibrium real interest rate (typically 2%)
    pi_t  = current inflation rate
    pi*   = inflation target (typically 2%)
    phi_pi = inflation response coefficient (Taylor: 1.5)
    phi_y  = output gap response coefficient (Taylor: 0.5)
    y_t   = output gap (percent deviation of GDP from potential)

Variants estimated:
1. **Original Taylor Rule**: fixed coefficients (phi_pi=1.5, phi_y=0.5)
2. **Estimated Taylor Rule**: OLS on i_t = c + a*pi_t + b*y_t + e_t
3. **Inertial Taylor Rule**: i_t = rho*i_{t-1} + (1-rho)*[r* + pi + phi_pi*(pi-pi*) + phi_y*y]
   Captures interest rate smoothing behavior.

Deviation tracking:
- Taylor-implied rate vs actual fed funds rate
- Sustained deviations indicate loose/tight monetary stance
- Cumulative deviation as a stance indicator

The Taylor Principle requires phi_pi > 1 (rates rise more than 1-for-1 with
inflation). Violation suggests accommodative monetary regime.

Score reflects deviation magnitude and Taylor principle violations.

Sources: FRED (FEDFUNDS, CPI inflation, CBO output gap, real-time GDP)
"""

import numpy as np

from app.layers.base import LayerBase


class TaylorRule(LayerBase):
    layer_id = "l2"
    name = "Taylor Rule"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        r_star = kwargs.get("r_star", 2.0)
        pi_star = kwargs.get("pi_star", 2.0)

        # Fetch data
        series_map = {
            "policy_rate": f"POLICY_RATE_{country}",
            "inflation": f"INFLATION_{country}",
            "output_gap": f"OUTPUT_GAP_{country}",
        }
        data = {}
        for label, code in series_map.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
                (code,),
            )
            if rows:
                data[label] = {r[0]: float(r[1]) for r in rows}

        if not data.get("policy_rate") or not data.get("inflation"):
            return {"score": 50, "results": {"error": "insufficient data"}}

        # Align series
        common = sorted(set(data["policy_rate"]) & set(data["inflation"]))
        if data.get("output_gap"):
            common = sorted(set(common) & set(data["output_gap"]))

        if len(common) < 10:
            return {"score": 50, "results": {"error": "too few overlapping observations"}}

        i_actual = np.array([data["policy_rate"][d] for d in common])
        pi = np.array([data["inflation"][d] for d in common])
        y_gap = (
            np.array([data["output_gap"][d] for d in common])
            if data.get("output_gap")
            else np.zeros(len(common))
        )

        results = {
            "country": country,
            "n_obs": len(common),
            "period": f"{common[0]} to {common[-1]}",
            "assumptions": {"r_star": r_star, "pi_star": pi_star},
        }

        # --- 1. Original Taylor Rule (fixed coefficients) ---
        phi_pi_orig = 1.5
        phi_y_orig = 0.5
        taylor_implied = r_star + pi + phi_pi_orig * (pi - pi_star) + phi_y_orig * y_gap
        deviation = i_actual - taylor_implied

        results["original"] = {
            "phi_pi": phi_pi_orig,
            "phi_y": phi_y_orig,
            "implied_rate": {
                "latest": float(taylor_implied[-1]),
                "series": taylor_implied.tolist(),
            },
            "actual_rate_latest": float(i_actual[-1]),
            "deviation": {
                "latest": float(deviation[-1]),
                "mean": float(np.mean(deviation)),
                "std": float(np.std(deviation, ddof=1)),
                "series": deviation.tolist(),
                "cumulative": float(np.sum(deviation)),
            },
            "dates": common,
        }

        # --- 2. Estimated Taylor Rule ---
        # i_t = c + a * pi_t + b * y_t + e_t
        n = len(i_actual)
        X = np.column_stack([np.ones(n), pi, y_gap])
        beta = np.linalg.lstsq(X, i_actual, rcond=None)[0]
        resid = i_actual - X @ beta
        sse = float(np.sum(resid ** 2))
        sst = float(np.sum((i_actual - np.mean(i_actual)) ** 2))
        r_squared = 1 - sse / sst if sst > 0 else 0.0

        # HC1 standard errors
        bread = np.linalg.inv(X.T @ X)
        meat = X.T @ np.diag(resid ** 2) @ X
        vcov = (n / (n - 3)) * bread @ meat @ bread
        se = np.sqrt(np.diag(vcov))

        # Implied phi_pi: the coefficient on inflation captures both the
        # direct effect and the Taylor principle channel
        est_phi_pi = float(beta[1])
        est_phi_y = float(beta[2])
        taylor_principle_holds = est_phi_pi > 1.0

        results["estimated"] = {
            "intercept": float(beta[0]),
            "phi_pi": est_phi_pi,
            "phi_pi_se": float(se[1]),
            "phi_y": est_phi_y,
            "phi_y_se": float(se[2]),
            "r_squared": round(r_squared, 4),
            "taylor_principle_holds": taylor_principle_holds,
            "fitted_values": (X @ beta).tolist(),
        }

        # --- 3. Inertial Taylor Rule ---
        # i_t = rho * i_{t-1} + (1-rho) * taylor_rate_t + e_t
        # Estimate rho via OLS: i_t = c0 + rho * i_{t-1} + c1 * pi_t + c2 * y_t + e_t
        i_lag = i_actual[:-1]
        i_curr = i_actual[1:]
        pi_curr = pi[1:]
        y_curr = y_gap[1:]
        n_iner = len(i_curr)

        X_iner = np.column_stack([np.ones(n_iner), i_lag, pi_curr, y_curr])
        beta_iner = np.linalg.lstsq(X_iner, i_curr, rcond=None)[0]
        resid_iner = i_curr - X_iner @ beta_iner
        sse_iner = float(np.sum(resid_iner ** 2))
        sst_iner = float(np.sum((i_curr - np.mean(i_curr)) ** 2))
        r2_iner = 1 - sse_iner / sst_iner if sst_iner > 0 else 0.0

        rho = float(beta_iner[1])

        # HC1 SE
        bread_i = np.linalg.inv(X_iner.T @ X_iner)
        meat_i = X_iner.T @ np.diag(resid_iner ** 2) @ X_iner
        vcov_i = (n_iner / (n_iner - 4)) * bread_i @ meat_i @ bread_i
        se_iner = np.sqrt(np.diag(vcov_i))

        # Long-run coefficients: phi_pi_lr = c1 / (1 - rho), phi_y_lr = c2 / (1 - rho)
        if abs(1 - rho) > 0.01:
            lr_phi_pi = float(beta_iner[2]) / (1 - rho)
            lr_phi_y = float(beta_iner[3]) / (1 - rho)
        else:
            lr_phi_pi = None
            lr_phi_y = None

        results["inertial"] = {
            "rho": round(rho, 3),
            "rho_se": round(float(se_iner[1]), 3),
            "short_run_phi_pi": float(beta_iner[2]),
            "short_run_phi_y": float(beta_iner[3]),
            "long_run_phi_pi": round(lr_phi_pi, 3) if lr_phi_pi is not None else None,
            "long_run_phi_y": round(lr_phi_y, 3) if lr_phi_y is not None else None,
            "r_squared": round(r2_iner, 4),
            "half_life_quarters": (
                round(float(np.log(0.5) / np.log(abs(rho))), 1) if 0 < abs(rho) < 1 else None
            ),
        }

        # --- Monetary stance assessment ---
        latest_dev = float(deviation[-1])
        if latest_dev < -1.0:
            stance = "loose"
        elif latest_dev > 1.0:
            stance = "tight"
        else:
            stance = "neutral"

        # Persistent deviations: count consecutive quarters of same sign
        sign = np.sign(deviation)
        consecutive = 1
        for j in range(len(sign) - 2, -1, -1):
            if sign[j] == sign[-1]:
                consecutive += 1
            else:
                break

        results["stance"] = {
            "current": stance,
            "latest_deviation_pp": round(latest_dev, 2),
            "consecutive_quarters_same_sign": consecutive,
        }

        # --- Score ---
        # Large deviations -> stress
        dev_penalty = min(abs(latest_dev) * 10, 40)
        # Taylor principle violation
        principle_penalty = 25 if not taylor_principle_holds else 0
        # Persistent deviation
        persist_penalty = min(consecutive * 2, 20)
        # Low R-squared in estimated rule
        fit_penalty = (1 - max(r_squared, 0)) * 15

        score = min(dev_penalty + principle_penalty + persist_penalty + fit_penalty, 100)

        return {"score": round(score, 1), "results": results}
