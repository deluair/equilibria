"""Food commodity price volatility analysis using GARCH models.

Models food price volatility using GARCH(1,1) and decomposes it into
fundamental (supply/demand) vs speculative components. Estimates price
volatility transmission from international to domestic markets.

Methodology:
    1. GARCH(1,1) for conditional volatility:
       r_t = mu + e_t,  e_t = sigma_t * z_t,  z_t ~ N(0,1)
       sigma_t^2 = omega + alpha * e_{t-1}^2 + beta * sigma_{t-1}^2

       where alpha = ARCH effect (shock persistence),
       beta = GARCH effect (volatility clustering),
       alpha + beta = persistence (< 1 for stationarity).

    2. Volatility decomposition:
       - Fundamental volatility: explained by supply shocks (weather, yield),
         demand shifts (income, population), and policy (tariffs, stocks).
       - Speculative/excess: residual volatility beyond fundamentals,
         proxied by detrended open interest or trading volume.

    3. International-to-domestic price transmission:
       p_domestic_t = alpha + beta * p_international_t + e_t
       where beta = pass-through elasticity.
       Error correction model for long-run relationship.

    Score: high unconditional volatility + high persistence + low stocks
    = food price stress.

References:
    Engle, R.F. (1982). "Autoregressive Conditional Heteroscedasticity
        with Estimates of the Variance of United Kingdom Inflation."
        Econometrica, 50(4), 987-1007.
    Bollerslev, T. (1986). "Generalized Autoregressive Conditional
        Heteroskedasticity." Journal of Econometrics, 31(3), 307-327.
    Gilbert, C.L. & Morgan, C.W. (2010). "Food price volatility."
        Phil. Trans. R. Soc. B, 365(1554), 3023-3034.
    Minot, N. (2011). "Transmission of World Food Price Changes to
        Markets in Sub-Saharan Africa." IFPRI Discussion Paper 01059.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FoodPriceVolatility(LayerBase):
    layer_id = "l5"
    name = "Food Price Volatility"

    # Key food commodities to track
    COMMODITIES = ["rice", "wheat", "maize", "soybeans", "sugar",
                   "palm_oil", "vegetable_oil"]

    async def compute(self, db, **kwargs) -> dict:
        """Estimate food price volatility with GARCH and decomposition.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            commodity : str - specific commodity (default all)
            lookback_months : int - history for GARCH (default 120)
        """
        country = kwargs.get("country_iso3", "BGD")
        commodity = kwargs.get("commodity")
        lookback = kwargs.get("lookback_months", 120)

        commodities = [commodity] if commodity else self.COMMODITIES

        results_by_commodity = {}
        all_persistence = []
        all_volatility = []

        for comm in commodities:
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value AS price, ds.metadata
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.source = 'food_prices'
                  AND ds.country_iso3 = ?
                  AND ds.description LIKE '%' || ? || '%'
                ORDER BY dp.date
                """,
                (country, comm),
            )

            if not rows or len(rows) < 36:
                continue

            prices = np.array([float(r["price"]) for r in rows if r["price"] and r["price"] > 0])
            if len(prices) < 36:
                continue

            # Compute log returns
            returns = np.diff(np.log(prices))
            n = len(returns)

            # Fit GARCH(1,1) via maximum likelihood
            garch = self._fit_garch11(returns)

            if garch is None:
                continue

            # Unconditional variance: omega / (1 - alpha - beta)
            omega, alpha, beta_g, mu = garch["omega"], garch["alpha"], garch["beta"], garch["mu"]
            persistence = alpha + beta_g
            unconditional_var = omega / (1 - persistence) if persistence < 1 else omega / 0.01
            annualized_vol = float(np.sqrt(unconditional_var * 12)) * 100  # annualized %

            # Conditional volatility series
            sigma2 = garch["conditional_variance"]
            current_vol = float(np.sqrt(sigma2[-1] * 12)) * 100

            # Volatility decomposition (simplified)
            # Parse fundamentals from metadata
            import json
            stock_to_use = []
            for r in rows:
                meta = json.loads(r["metadata"]) if r.get("metadata") else {}
                stu = meta.get("stock_to_use_ratio")
                if stu is not None:
                    stock_to_use.append(float(stu))

            fundamental_share = None
            speculative_share = None
            if len(stock_to_use) >= 12:
                stu_arr = np.array(stock_to_use[-n:]) if len(stock_to_use) >= n else np.array(stock_to_use)
                # Regress squared returns on fundamentals
                if len(stu_arr) == n:
                    fund_result = self._volatility_decomposition(returns ** 2, stu_arr)
                    if fund_result:
                        fundamental_share = fund_result["r_squared"]
                        speculative_share = 1.0 - fundamental_share

            # Price transmission (if international prices available)
            intl_rows = await db.fetch_all(
                """
                SELECT dp.value AS price
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.source = 'food_prices_international'
                  AND ds.description LIKE '%' || ? || '%'
                ORDER BY dp.date
                """,
                (comm,),
            )

            transmission = None
            if intl_rows and len(intl_rows) >= 24:
                intl_prices = np.array([float(r["price"]) for r in intl_rows if r["price"] and r["price"] > 0])
                min_len = min(len(prices), len(intl_prices))
                if min_len >= 24:
                    transmission = self._price_transmission(
                        prices[-min_len:], intl_prices[-min_len:]
                    )

            all_persistence.append(persistence)
            all_volatility.append(annualized_vol)

            results_by_commodity[comm] = {
                "garch_params": {
                    "mu": round(float(mu), 6),
                    "omega": round(float(omega), 8),
                    "alpha": round(float(alpha), 4),
                    "beta": round(float(beta_g), 4),
                    "persistence": round(float(persistence), 4),
                },
                "annualized_volatility_pct": round(annualized_vol, 2),
                "current_volatility_pct": round(current_vol, 2),
                "n_observations": n,
                "volatility_decomposition": {
                    "fundamental_share": round(float(fundamental_share), 3) if fundamental_share is not None else None,
                    "speculative_share": round(float(speculative_share), 3) if speculative_share is not None else None,
                },
                "price_transmission": transmission,
                "mean_stock_to_use": round(float(np.mean(stock_to_use)), 3) if stock_to_use else None,
            }

        if not results_by_commodity:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient food price data"}

        # Aggregate score
        avg_vol = float(np.mean(all_volatility))
        avg_persistence = float(np.mean(all_persistence))

        # High volatility (>30% annualized) and high persistence (>0.95) = stress
        vol_score = float(np.clip(avg_vol / 50 * 60, 0, 60))
        persist_score = float(np.clip((avg_persistence - 0.8) / 0.2 * 40, 0, 40))
        score = float(np.clip(vol_score + persist_score, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_commodities_analyzed": len(results_by_commodity),
            "aggregate": {
                "mean_annualized_volatility_pct": round(avg_vol, 2),
                "mean_persistence": round(avg_persistence, 4),
            },
            "commodities": results_by_commodity,
        }

    @staticmethod
    def _fit_garch11(returns: np.ndarray) -> dict | None:
        """Fit GARCH(1,1) via quasi-maximum likelihood.

        Uses a simple grid initialization + iterative update approach
        without requiring the arch package.
        """
        n = len(returns)
        if n < 20:
            return None

        mu = float(np.mean(returns))
        eps = returns - mu
        eps2 = eps ** 2
        sample_var = float(np.var(returns))

        # Grid search for initial parameters
        best_ll = -np.inf
        best_params = None

        for alpha in [0.05, 0.10, 0.15, 0.20]:
            for beta in [0.70, 0.75, 0.80, 0.85, 0.90]:
                if alpha + beta >= 1.0:
                    continue
                omega = sample_var * (1 - alpha - beta)
                if omega <= 0:
                    continue

                # Compute conditional variance series
                sigma2 = np.zeros(n)
                sigma2[0] = sample_var
                for t in range(1, n):
                    sigma2[t] = omega + alpha * eps2[t - 1] + beta * sigma2[t - 1]
                    sigma2[t] = max(sigma2[t], 1e-10)

                # Log-likelihood (Gaussian)
                ll = -0.5 * np.sum(np.log(sigma2) + eps2 / sigma2)
                if ll > best_ll:
                    best_ll = ll
                    best_params = (omega, alpha, beta)

        if best_params is None:
            return None

        omega, alpha, beta = best_params

        # Refine with simple gradient-free optimization (Nelder-Mead style coordinate descent)
        for _ in range(200):
            sigma2 = np.zeros(n)
            sigma2[0] = sample_var
            for t in range(1, n):
                sigma2[t] = omega + alpha * eps2[t - 1] + beta * sigma2[t - 1]
                sigma2[t] = max(sigma2[t], 1e-10)

            ll = -0.5 * np.sum(np.log(sigma2) + eps2 / sigma2)

            # Numerical gradient steps
            delta = 1e-5
            improved = False
            for param_idx in range(3):
                params_list = [omega, alpha, beta]
                params_list[param_idx] += delta
                o_t, a_t, b_t = params_list
                if a_t + b_t >= 1.0 or o_t <= 0 or a_t < 0 or b_t < 0:
                    continue
                s2 = np.zeros(n)
                s2[0] = sample_var
                for t in range(1, n):
                    s2[t] = o_t + a_t * eps2[t - 1] + b_t * s2[t - 1]
                    s2[t] = max(s2[t], 1e-10)
                ll_new = -0.5 * np.sum(np.log(s2) + eps2 / s2)
                grad = (ll_new - ll) / delta
                step = min(abs(grad) * 1e-4, delta * 10) * np.sign(grad)
                new_val = params_list[param_idx] - delta + step
                if new_val > 0:
                    params_list[param_idx] = new_val
                    if params_list[1] + params_list[2] < 1.0:
                        omega, alpha, beta = params_list
                        improved = True

            if not improved:
                break

        # Final conditional variance
        sigma2 = np.zeros(n)
        sigma2[0] = sample_var
        for t in range(1, n):
            sigma2[t] = omega + alpha * eps2[t - 1] + beta * sigma2[t - 1]
            sigma2[t] = max(sigma2[t], 1e-10)

        return {
            "omega": omega, "alpha": alpha, "beta": beta, "mu": mu,
            "conditional_variance": sigma2,
            "log_likelihood": float(-0.5 * np.sum(np.log(sigma2) + eps2 / sigma2)),
        }

    @staticmethod
    def _volatility_decomposition(
        squared_returns: np.ndarray, fundamentals: np.ndarray
    ) -> dict | None:
        """Regress squared returns on fundamental factors."""
        try:
            n = min(len(squared_returns), len(fundamentals))
            y = squared_returns[:n]
            X = np.column_stack([np.ones(n), fundamentals[:n]])
            beta = np.linalg.lstsq(X, y, rcond=None)[0]
            fitted = X @ beta
            ss_res = np.sum((y - fitted) ** 2)
            ss_tot = np.sum((y - y.mean()) ** 2)
            r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
            return {"r_squared": max(0, r2), "coefficients": beta.tolist()}
        except Exception:
            return None

    @staticmethod
    def _price_transmission(
        domestic: np.ndarray, international: np.ndarray
    ) -> dict | None:
        """Estimate price transmission elasticity with error correction.

        Short-run: dp_d = alpha + beta_sr * dp_i + gamma * ecm_{t-1} + e
        where ecm = p_d - theta * p_i (long-run relationship)
        """
        try:
            n = min(len(domestic), len(international))
            p_d = np.log(domestic[:n])
            p_i = np.log(international[:n])

            # Long-run cointegrating relationship
            X_lr = np.column_stack([np.ones(n), p_i])
            theta_lr = np.linalg.lstsq(X_lr, p_d, rcond=None)[0]
            ecm = p_d - X_lr @ theta_lr  # error correction term

            # Short-run ECM
            dp_d = np.diff(p_d)
            dp_i = np.diff(p_i)
            ecm_lag = ecm[:-1]
            m = len(dp_d)

            X_sr = np.column_stack([np.ones(m), dp_i, ecm_lag])
            beta_sr = np.linalg.lstsq(X_sr, dp_d, rcond=None)[0]

            fitted = X_sr @ beta_sr
            ss_res = np.sum((dp_d - fitted) ** 2)
            ss_tot = np.sum((dp_d - dp_d.mean()) ** 2)
            r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

            return {
                "long_run_elasticity": round(float(theta_lr[1]), 4),
                "short_run_elasticity": round(float(beta_sr[1]), 4),
                "ecm_speed_of_adjustment": round(float(beta_sr[2]), 4),
                "r_squared": round(float(r2), 4),
                "half_life_months": (
                    round(float(-np.log(2) / np.log(1 + beta_sr[2])), 1)
                    if -1 < beta_sr[2] < 0 else None
                ),
            }
        except Exception:
            return None
