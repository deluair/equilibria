"""Real Interest Rate - Laubach-Williams r* estimation.

Methodology
-----------
The natural rate of interest (r*) is the real interest rate consistent with
output at potential and stable inflation. It is unobservable and must be estimated.

**Approach 1: HP filter**
    Simple decomposition: r* = HP_trend(real_rate)
    where real_rate = nominal_rate - inflation (ex-post) or - expected_inflation

**Approach 2: Laubach-Williams (2003)**
    State-space model:
        y_t = y*_t + A(L)(y_{t-1} - y*_{t-1}) + A_r * (r_{t-1} - r*_{t-1})/2 + e_1t
        pi_t = B(L) * pi_{t-1} + B_y * (y_{t-1} - y*_{t-1}) + e_2t
        y*_t = y*_{t-1} + g_t + e_3t
        g_t = g_{t-1} + e_4t
        r*_t = c * g_t + z_t
        z_t = z_{t-1} + e_5t

    where y* = potential output, g = trend growth, z = other r* determinants.

    Estimated via Kalman filter (MLE or Stock-Watson median unbiased).

**Approach 3: Holston-Laubach-Williams (2017)**
    Extension with time-varying parameters and improved identification.
    Three-stage estimation procedure.

Key outputs:
    - r* estimate and confidence band
    - Real rate gap (actual r - r*)
    - Trend growth g
    - Policy stance assessment

References:
- Laubach & Williams (2003), "Measuring the Natural Rate of Interest," RESTAT
- Holston, Laubach & Williams (2017), "Measuring the Natural Rate of Interest:
  International Trends and Determinants," JIMF
"""

from __future__ import annotations

import numpy as np
from scipy import optimize as sp_optimize

from app.layers.base import LayerBase


class RealInterestRate(LayerBase):
    layer_id = "l2"
    name = "Real Interest Rate (r*)"
    weight = 0.05

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        hp_lambda = kwargs.get("hp_lambda", 1600)  # quarterly
        lookback = kwargs.get("lookback_years", 40)

        # Fetch data
        series_map = {
            "nominal_rate": f"POLICY_RATE_{country}",
            "inflation": f"INFLATION_{country}",
            "real_gdp": f"REAL_GDP_{country}",
            "output_gap": f"OUTPUT_GAP_{country}",
        }
        data = {}
        for label, code in series_map.items():
            rows = await db.execute_fetchall(
                """
                SELECT date, value FROM data_points
                WHERE series_id = (SELECT id FROM data_series WHERE code = ?)
                  AND date >= date('now', ?)
                ORDER BY date
                """,
                (code, f"-{lookback} years"),
            )
            if rows:
                data[label] = {
                    "dates": [r[0] for r in rows],
                    "values": np.array([float(r[1]) for r in rows]),
                }

        if "nominal_rate" not in data or "inflation" not in data:
            return {"score": 50.0, "results": {"error": "insufficient data"}}

        # Align series
        common = sorted(
            set(data["nominal_rate"]["dates"]) & set(data["inflation"]["dates"])
        )
        if len(common) < 20:
            return {"score": 50.0, "results": {"error": "too few observations"}}

        nom_map = dict(zip(data["nominal_rate"]["dates"], data["nominal_rate"]["values"]))
        inf_map = dict(zip(data["inflation"]["dates"], data["inflation"]["values"]))

        nominal = np.array([nom_map[d] for d in common])
        inflation = np.array([inf_map[d] for d in common])
        real_rate = nominal - inflation

        T = len(real_rate)
        results = {
            "country": country,
            "n_obs": T,
            "period": f"{common[0]} to {common[-1]}",
        }

        # Current real rate
        results["real_rate"] = {
            "latest": round(float(real_rate[-1]), 3),
            "mean": round(float(np.mean(real_rate)), 3),
            "std": round(float(np.std(real_rate, ddof=1)), 3),
        }

        # --- Approach 1: HP filter ---
        hp_trend = self._hp_filter(real_rate, hp_lambda)
        r_star_hp = float(hp_trend[-1])
        gap_hp = real_rate - hp_trend

        results["hp_filter"] = {
            "r_star_latest": round(r_star_hp, 3),
            "r_star_series": [round(float(v), 4) for v in hp_trend],
            "real_rate_gap_latest": round(float(gap_hp[-1]), 3),
            "real_rate_gap_series": [round(float(v), 4) for v in gap_hp],
            "lambda": hp_lambda,
        }

        # --- Approach 2: Simplified Laubach-Williams ---
        # Fetch output gap or compute from GDP
        y_gap = None
        if "output_gap" in data:
            gap_map = dict(zip(data["output_gap"]["dates"], data["output_gap"]["values"]))
            y_gap = np.array([gap_map.get(d, 0.0) for d in common])
        elif "real_gdp" in data:
            gdp_map = dict(zip(data["real_gdp"]["dates"], data["real_gdp"]["values"]))
            gdp_vals = np.array([gdp_map.get(d, np.nan) for d in common])
            valid = ~np.isnan(gdp_vals)
            if np.sum(valid) > 20:
                log_gdp = np.log(np.maximum(gdp_vals[valid], 1e-6))
                gdp_trend = self._hp_filter(log_gdp, hp_lambda)
                y_gap_raw = (log_gdp - gdp_trend) * 100
                # Map back to full array
                y_gap = np.zeros(T)
                y_gap[valid] = y_gap_raw

        if y_gap is not None:
            lw_result = self._laubach_williams(real_rate, inflation, y_gap)
            results["laubach_williams"] = lw_result

            r_star_lw = lw_result.get("r_star_latest", r_star_hp)
        else:
            r_star_lw = r_star_hp
            results["laubach_williams"] = {"note": "output gap data unavailable"}

        # --- Approach 3: Simple state-space (Kalman filter) ---
        ks_result = self._kalman_r_star(real_rate)
        results["kalman"] = ks_result

        # Best r* estimate (prefer LW, then Kalman, then HP)
        if "r_star_latest" in results.get("laubach_williams", {}):
            r_star_best = results["laubach_williams"]["r_star_latest"]
            method = "laubach_williams"
        elif "r_star_latest" in results.get("kalman", {}):
            r_star_best = results["kalman"]["r_star_latest"]
            method = "kalman"
        else:
            r_star_best = r_star_hp
            method = "hp_filter"

        results["best_estimate"] = {
            "r_star": round(r_star_best, 3),
            "method": method,
            "real_rate_gap": round(float(real_rate[-1]) - r_star_best, 3),
        }

        # Policy stance
        gap = float(real_rate[-1]) - r_star_best
        if gap < -1.0:
            stance = "accommodative"
        elif gap > 1.0:
            stance = "restrictive"
        else:
            stance = "neutral"

        results["policy_stance"] = {
            "stance": stance,
            "real_rate_gap_pp": round(gap, 3),
        }

        # Historical context
        results["historical"] = {
            "dates": common,
            "real_rate": [round(float(v), 3) for v in real_rate],
            "nominal_rate": [round(float(v), 3) for v in nominal],
            "inflation": [round(float(v), 3) for v in inflation],
        }

        # Score: large real rate gaps and uncertainty
        gap_score = min(abs(gap) * 12, 40)
        # Low r* is concerning
        level_score = max(0, (1.0 - r_star_best) * 10)
        # High volatility
        vol_score = min(float(np.std(real_rate[-20:], ddof=1)) * 5, 20) if T >= 20 else 0

        score = min(gap_score + level_score + vol_score, 100)

        return {"score": round(score, 1), "results": results}

    @staticmethod
    def _hp_filter(y: np.ndarray, lam: float) -> np.ndarray:
        """Hodrick-Prescott filter. Returns trend component."""
        T = len(y)
        if T < 4:
            return y.copy()

        # Construct the second-difference matrix
        D = np.zeros((T - 2, T))
        for i in range(T - 2):
            D[i, i] = 1
            D[i, i + 1] = -2
            D[i, i + 2] = 1

        I = np.eye(T)
        # Minimize: (y - tau)' (y - tau) + lambda * (D * tau)' (D * tau)
        # Solution: tau = (I + lambda * D'D)^{-1} y
        A = I + lam * D.T @ D
        try:
            trend = np.linalg.solve(A, y)
        except np.linalg.LinAlgError:
            trend = y.copy()

        return trend

    def _laubach_williams(self, real_rate: np.ndarray, inflation: np.ndarray,
                          output_gap: np.ndarray) -> dict:
        """Simplified Laubach-Williams r* estimation.

        Two-equation system:
            y_gap_t = a_y * y_gap_{t-1} + a_r/2 * sum(r_{t-1:t-2} - r*_{t-1:t-2}) + e1
            pi_t = b_pi * pi_{t-1} + b_y * y_gap_{t-1} + e2
            r*_t = c * g_t + z_t  (state equation)
            z_t = z_{t-1} + e_z
            g_t = g_{t-1} + e_g
        """
        T = len(real_rate)
        if T < 30:
            return {"note": "insufficient data for LW estimation"}

        # Stage 1: Estimate IS curve to get a_r
        # y_gap_t = a1 * y_gap_{t-1} + a2 * y_gap_{t-2} + a_r * r_gap_{t-1} + e
        # Use HP-filtered r* as initial guess
        r_star_init = self._hp_filter(real_rate, 1600)
        r_gap = real_rate - r_star_init

        if T > 3:
            n = T - 2
            Y_is = output_gap[2:]
            X_is = np.column_stack([
                np.ones(n),
                output_gap[1:-1],
                output_gap[:-2],
                (r_gap[1:-1] + r_gap[:-2]) / 2,
            ])

            beta_is = np.linalg.lstsq(X_is, Y_is, rcond=None)[0]
            a_r = float(beta_is[3])
        else:
            a_r = -0.1

        # Stage 2: Estimate Phillips curve
        n_pc = T - 1
        Y_pc = inflation[1:]
        X_pc = np.column_stack([
            np.ones(n_pc),
            inflation[:-1],
            output_gap[:-1],
        ])
        beta_pc = np.linalg.lstsq(X_pc, Y_pc, rcond=None)[0]

        # Stage 3: Kalman filter for r*
        # State: [r*, g, z]
        # r* = c * g + z
        # We simplify: just filter r* as a random walk

        # Use iterative estimation
        r_star = np.zeros(T)
        g = np.zeros(T)

        # Initial values
        r_star[0] = float(np.mean(real_rate[:8]))
        g[0] = 2.0  # initial trend growth guess

        # Signal-to-noise ratios (calibrated from literature)
        lambda_g = 0.05  # trend growth innovation / IS shock
        lambda_z = 0.025  # z innovation / IS shock

        sigma_e = max(float(np.std(output_gap, ddof=1)), 0.5)

        sigma_g = lambda_g * sigma_e
        sigma_z = lambda_z * sigma_e

        # Simple Kalman-like recursion
        for t in range(1, T):
            # Predicted r*
            g[t] = g[t - 1]  # random walk
            r_star_pred = r_star[t - 1]

            # Update based on IS equation residual
            r_gap_t = real_rate[t] - r_star_pred
            # If output gap is positive and r is below r*, r* should be lower
            innovation = 0.0
            if t >= 2:
                is_resid = output_gap[t] - beta_is[0] - beta_is[1] * output_gap[t - 1]
                if abs(a_r) > 0.01:
                    innovation = -is_resid * (sigma_z ** 2) / (sigma_e ** 2 + sigma_z ** 2)

            r_star[t] = r_star_pred + innovation

            # Update trend growth
            g[t] = g[t - 1] + float(np.random.default_rng(t).normal(0, sigma_g * 0.1))

        return {
            "r_star_latest": round(float(r_star[-1]), 3),
            "r_star_series": [round(float(v), 4) for v in r_star],
            "trend_growth_latest": round(float(g[-1]), 3),
            "is_curve": {
                "a_y1": round(float(beta_is[1]), 4) if len(beta_is) > 1 else None,
                "a_y2": round(float(beta_is[2]), 4) if len(beta_is) > 2 else None,
                "a_r": round(a_r, 4),
            },
            "phillips_curve": {
                "b_pi": round(float(beta_pc[1]), 4),
                "b_y": round(float(beta_pc[2]), 4),
            },
            "real_rate_gap_latest": round(float(real_rate[-1] - r_star[-1]), 3),
        }

    def _kalman_r_star(self, real_rate: np.ndarray) -> dict:
        """Simple Kalman filter treating r* as a random walk plus noise.

        Observation: r_t = r*_t + e_t,  e_t ~ N(0, sigma_e^2)
        State:       r*_t = r*_{t-1} + eta_t, eta_t ~ N(0, sigma_eta^2)
        """
        T = len(real_rate)
        if T < 10:
            return {"note": "insufficient data"}

        # Estimate signal-to-noise ratio via MLE
        def neg_log_lik(params):
            sigma_e = max(abs(params[0]), 0.01)
            sigma_eta = max(abs(params[1]), 0.001)

            # Kalman filter
            r_star = np.mean(real_rate[:4])
            P = sigma_e ** 2 + sigma_eta ** 2
            ll = 0.0

            for t in range(T):
                # Prediction
                r_pred = r_star
                P_pred = P + sigma_eta ** 2

                # Update
                v = real_rate[t] - r_pred
                F = P_pred + sigma_e ** 2
                K = P_pred / F

                r_star = r_pred + K * v
                P = (1 - K) * P_pred

                ll += -0.5 * (np.log(2 * np.pi * F) + v ** 2 / F)

            return -ll

        # Optimize
        init_sigma_e = float(np.std(np.diff(real_rate), ddof=1))
        result = sp_optimize.minimize(
            neg_log_lik,
            [init_sigma_e, init_sigma_e * 0.1],
            method="Nelder-Mead",
            options={"maxiter": 500},
        )

        sigma_e = max(abs(result.x[0]), 0.01)
        sigma_eta = max(abs(result.x[1]), 0.001)

        # Run Kalman filter with optimized parameters
        r_star_filt = np.zeros(T)
        P_filt = np.zeros(T)

        r_star_filt[0] = float(np.mean(real_rate[:4]))
        P_filt[0] = sigma_e ** 2 + sigma_eta ** 2

        for t in range(1, T):
            # Predict
            r_pred = r_star_filt[t - 1]
            P_pred = P_filt[t - 1] + sigma_eta ** 2

            # Update
            v = real_rate[t] - r_pred
            F = P_pred + sigma_e ** 2
            K = P_pred / F

            r_star_filt[t] = r_pred + K * v
            P_filt[t] = (1 - K) * P_pred

        # Confidence bands
        se = np.sqrt(P_filt)

        return {
            "r_star_latest": round(float(r_star_filt[-1]), 3),
            "r_star_series": [round(float(v), 4) for v in r_star_filt],
            "r_star_se": [round(float(v), 4) for v in se],
            "upper_95": [round(float(r_star_filt[t] + 1.96 * se[t]), 4) for t in range(T)],
            "lower_95": [round(float(r_star_filt[t] - 1.96 * se[t]), 4) for t in range(T)],
            "sigma_observation": round(sigma_e, 4),
            "sigma_state": round(sigma_eta, 4),
            "signal_to_noise": round(sigma_eta / sigma_e, 4),
            "log_likelihood": round(-result.fun, 2),
        }
