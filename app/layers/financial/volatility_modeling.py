"""Volatility modeling and forecasting.

GARCH(1,1) for conditional variance dynamics, EGARCH for asymmetric leverage
effects, GJR-GARCH for threshold asymmetry. Realized volatility estimation
from high-frequency data. Volatility forecasting with multi-step ahead
prediction.

Score (0-100): based on current vs historical volatility and persistence.
High volatility or near-unit-root persistence pushes toward CRISIS.
"""

from __future__ import annotations

import numpy as np
from scipy.optimize import minimize

from app.layers.base import LayerBase


class VolatilityModeling(LayerBase):
    layer_id = "l7"
    name = "Volatility Modeling"

    async def compute(self, db, **kwargs) -> dict:
        asset_id = kwargs.get("asset_id", "market_index")
        country = kwargs.get("country_iso3", "USA")
        lookback = kwargs.get("lookback_years", 5)
        forecast_horizon = kwargs.get("forecast_horizon", 22)  # trading days

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('fred', 'yahoo', 'asset_returns')
              AND ds.country_iso3 = ?
              AND ds.description LIKE ?
              AND dp.date >= date('now', ?)
            ORDER BY dp.date
            """,
            (country, f"%{asset_id}%", f"-{lookback} years"),
        )

        if not rows or len(rows) < 60:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient return data"}

        returns = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]
        n = len(returns)

        # Demean returns
        mu = float(np.mean(returns))
        eps = returns - mu

        # GARCH(1,1)
        garch = self._fit_garch(eps)

        # EGARCH
        egarch = self._fit_egarch(eps)

        # GJR-GARCH
        gjr = self._fit_gjr_garch(eps)

        # Realized volatility (if sub-periods available, use rolling windows)
        rv_5d = self._rolling_realized_vol(returns, 5)
        rv_22d = self._rolling_realized_vol(returns, 22)

        # Volatility forecasts from GARCH(1,1)
        garch_forecasts = None
        if garch and garch.get("converged"):
            garch_forecasts = self._garch_forecast(
                garch["omega"], garch["alpha"], garch["beta"],
                garch["conditional_var"][-1], forecast_horizon,
            )

        # Current annualized vol
        current_vol = float(np.std(returns[-22:], ddof=1) * np.sqrt(252)) if n >= 22 else None
        historical_vol = float(np.std(returns, ddof=1) * np.sqrt(252))

        # Vol regime: compare current to historical percentile
        rolling_vols = []
        for i in range(22, n):
            rolling_vols.append(np.std(returns[i - 22:i], ddof=1) * np.sqrt(252))
        vol_percentile = None
        if current_vol and rolling_vols:
            vol_percentile = float(np.mean(np.array(rolling_vols) <= current_vol) * 100)

        # Score: high vol + high persistence = stress
        vol_ratio = current_vol / max(historical_vol, 1e-10) if current_vol else 1.0
        vol_component = float(np.clip((vol_ratio - 1.0) * 50.0 + 30.0, 0, 100))

        persistence = garch["alpha"] + garch["beta"] if garch else 0.9
        persist_component = float(np.clip(persistence * 80.0, 0, 100))

        # Asymmetry from EGARCH
        asym_component = 30.0
        if egarch and egarch.get("converged"):
            gamma = abs(egarch.get("gamma", 0))
            asym_component = float(np.clip(gamma * 100.0, 0, 100))

        score = float(np.clip(
            0.45 * vol_component + 0.35 * persist_component + 0.20 * asym_component,
            0, 100,
        ))

        return {
            "score": round(score, 2),
            "country": country,
            "asset_id": asset_id,
            "n_obs": n,
            "garch_1_1": {
                "omega": round(garch["omega"], 8),
                "alpha": round(garch["alpha"], 6),
                "beta": round(garch["beta"], 6),
                "persistence": round(garch["alpha"] + garch["beta"], 6),
                "unconditional_var": round(garch["uncond_var"], 8),
                "unconditional_vol_ann": round(
                    float(np.sqrt(garch["uncond_var"] * 252)), 4),
                "log_likelihood": round(garch["log_lik"], 2),
                "converged": garch["converged"],
            } if garch else None,
            "egarch": {
                "omega": round(egarch["omega"], 6),
                "alpha": round(egarch["alpha"], 6),
                "gamma": round(egarch["gamma"], 6),
                "beta": round(egarch["beta"], 6),
                "leverage_effect": egarch["gamma"] < 0,
                "converged": egarch["converged"],
            } if egarch else None,
            "gjr_garch": {
                "omega": round(gjr["omega"], 8),
                "alpha": round(gjr["alpha"], 6),
                "gamma": round(gjr["gamma"], 6),
                "beta": round(gjr["beta"], 6),
                "persistence": round(gjr["alpha"] + gjr["beta"] + 0.5 * gjr["gamma"], 6),
                "converged": gjr["converged"],
            } if gjr else None,
            "realized_volatility": {
                "rv_5d_ann": round(rv_5d[-1] * np.sqrt(252), 4) if rv_5d else None,
                "rv_22d_ann": round(rv_22d[-1] * np.sqrt(252), 4) if rv_22d else None,
            },
            "current_vol_ann": round(current_vol, 4) if current_vol else None,
            "historical_vol_ann": round(historical_vol, 4),
            "vol_percentile": round(vol_percentile, 1) if vol_percentile else None,
            "forecast": {
                "horizon_days": forecast_horizon,
                "garch_vol_forecast_ann": [round(float(f), 4) for f in garch_forecasts]
                if garch_forecasts is not None else None,
            },
        }

    @staticmethod
    def _fit_garch(eps: np.ndarray) -> dict | None:
        """Fit GARCH(1,1): sigma2(t) = omega + alpha*eps(t-1)^2 + beta*sigma2(t-1).

        MLE under Gaussian innovations.
        """
        n = len(eps)
        if n < 30:
            return None

        var_eps = float(np.var(eps))

        def neg_log_lik(params):
            omega, alpha, beta = params
            if omega <= 0 or alpha < 0 or beta < 0 or alpha + beta >= 1.0:
                return 1e10
            sigma2 = np.zeros(n)
            sigma2[0] = omega / max(1 - alpha - beta, 0.01)
            for t in range(1, n):
                sigma2[t] = omega + alpha * eps[t - 1] ** 2 + beta * sigma2[t - 1]
                sigma2[t] = max(sigma2[t], 1e-10)
            ll = -0.5 * np.sum(np.log(2 * np.pi) + np.log(sigma2) + eps ** 2 / sigma2)
            return -ll

        x0 = [var_eps * 0.05, 0.08, 0.88]
        bounds = [(1e-10, None), (1e-10, 0.5), (1e-10, 0.999)]

        result = minimize(neg_log_lik, x0, method="L-BFGS-B", bounds=bounds,
                          options={"maxiter": 1000})

        omega, alpha, beta = result.x
        uncond_var = omega / max(1 - alpha - beta, 0.001)

        # Reconstruct conditional variance
        sigma2 = np.zeros(n)
        sigma2[0] = uncond_var
        for t in range(1, n):
            sigma2[t] = omega + alpha * eps[t - 1] ** 2 + beta * sigma2[t - 1]
            sigma2[t] = max(sigma2[t], 1e-10)

        return {
            "omega": float(omega),
            "alpha": float(alpha),
            "beta": float(beta),
            "uncond_var": float(uncond_var),
            "log_lik": float(-result.fun),
            "conditional_var": sigma2,
            "converged": result.success,
        }

    @staticmethod
    def _fit_egarch(eps: np.ndarray) -> dict | None:
        """Fit EGARCH(1,1): log(sigma2(t)) = omega + alpha*g(z) + beta*log(sigma2(t-1))
        where g(z) = z + gamma*(|z| - E|z|), z = eps/sigma.
        """
        n = len(eps)
        if n < 30:
            return None

        log_var = np.log(max(np.var(eps), 1e-10))

        def neg_log_lik(params):
            omega, alpha, gamma, beta = params
            if abs(beta) >= 1.0:
                return 1e10
            log_s2 = np.zeros(n)
            log_s2[0] = omega / max(1 - beta, 0.01)
            e_abs_z = np.sqrt(2.0 / np.pi)  # E[|z|] for standard normal
            for t in range(1, n):
                s2_prev = np.exp(log_s2[t - 1])
                z = eps[t - 1] / max(np.sqrt(s2_prev), 1e-10)
                g_z = alpha * z + gamma * (abs(z) - e_abs_z)
                log_s2[t] = omega + g_z + beta * log_s2[t - 1]
                log_s2[t] = max(min(log_s2[t], 20), -20)

            sigma2 = np.exp(log_s2)
            ll = -0.5 * np.sum(np.log(2 * np.pi) + log_s2 + eps ** 2 / sigma2)
            return -ll

        x0 = [log_var * 0.05, 0.15, -0.05, 0.95]

        result = minimize(neg_log_lik, x0, method="Nelder-Mead",
                          options={"maxiter": 2000})

        omega, alpha, gamma, beta = result.x

        return {
            "omega": float(omega),
            "alpha": float(alpha),
            "gamma": float(gamma),
            "beta": float(beta),
            "converged": result.success,
        }

    @staticmethod
    def _fit_gjr_garch(eps: np.ndarray) -> dict | None:
        """Fit GJR-GARCH(1,1): sigma2(t) = omega + alpha*eps^2 + gamma*eps^2*I(eps<0) + beta*sigma2.

        Glosten-Jagannathan-Runkle threshold model for asymmetric volatility.
        """
        n = len(eps)
        if n < 30:
            return None

        var_eps = float(np.var(eps))

        def neg_log_lik(params):
            omega, alpha, gamma, beta = params
            if omega <= 0 or alpha < 0 or gamma < -alpha or beta < 0:
                return 1e10
            if alpha + beta + 0.5 * gamma >= 1.0:
                return 1e10

            sigma2 = np.zeros(n)
            sigma2[0] = omega / max(1 - alpha - beta - 0.5 * gamma, 0.01)
            for t in range(1, n):
                indicator = 1.0 if eps[t - 1] < 0 else 0.0
                sigma2[t] = (omega + alpha * eps[t - 1] ** 2
                             + gamma * eps[t - 1] ** 2 * indicator
                             + beta * sigma2[t - 1])
                sigma2[t] = max(sigma2[t], 1e-10)

            ll = -0.5 * np.sum(np.log(2 * np.pi) + np.log(sigma2) + eps ** 2 / sigma2)
            return -ll

        x0 = [var_eps * 0.05, 0.05, 0.05, 0.88]
        bounds = [(1e-10, None), (1e-10, 0.5), (0, 0.5), (1e-10, 0.999)]

        result = minimize(neg_log_lik, x0, method="L-BFGS-B", bounds=bounds,
                          options={"maxiter": 1000})

        omega, alpha, gamma, beta = result.x

        return {
            "omega": float(omega),
            "alpha": float(alpha),
            "gamma": float(gamma),
            "beta": float(beta),
            "converged": result.success,
        }

    @staticmethod
    def _rolling_realized_vol(returns: np.ndarray, window: int) -> list[float]:
        """Rolling realized volatility (standard deviation of returns)."""
        if len(returns) < window:
            return []
        rv = []
        for i in range(window, len(returns) + 1):
            rv.append(float(np.std(returns[i - window:i], ddof=1)))
        return rv

    @staticmethod
    def _garch_forecast(omega: float, alpha: float, beta: float,
                        sigma2_last: float, horizon: int) -> list[float]:
        """Multi-step GARCH(1,1) variance forecast.

        E[sigma2(t+h)] = uncond_var + (alpha+beta)^h * (sigma2(t) - uncond_var)
        Annualized volatility for each step.
        """
        uncond_var = omega / max(1 - alpha - beta, 0.001)
        persistence = alpha + beta
        forecasts = []
        for h in range(1, horizon + 1):
            var_h = uncond_var + persistence ** h * (sigma2_last - uncond_var)
            vol_ann = float(np.sqrt(max(var_h, 1e-10) * 252))
            forecasts.append(vol_ann)
        return forecasts
