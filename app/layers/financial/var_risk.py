"""Value at Risk and Expected Shortfall estimation.

Implements three VaR methodologies: historical simulation, parametric
(variance-covariance with normal and Student-t), and Monte Carlo simulation.
Expected Shortfall (CVaR) as the coherent risk measure complement. Kupiec
(1995) unconditional coverage test and Christoffersen (1998) conditional
coverage test for backtesting.

Score (0-100): based on VaR breach frequency vs expected, and ES magnitude.
Frequent breaches or large tail losses push toward CRISIS.
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats
from app.layers.base import LayerBase


class ValueAtRisk(LayerBase):
    layer_id = "l7"
    name = "Value at Risk"

    async def compute(self, db, **kwargs) -> dict:
        portfolio_id = kwargs.get("portfolio_id", "market_index")
        country = kwargs.get("country_iso3", "USA")
        confidence = kwargs.get("confidence", 0.99)
        horizon = kwargs.get("horizon_days", 1)
        n_simulations = kwargs.get("n_simulations", 10000)
        lookback = kwargs.get("lookback_years", 5)

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
            (country, f"%{portfolio_id}%", f"-{lookback} years"),
        )

        if not rows or len(rows) < 60:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient return data"}

        returns = np.array([float(r["value"]) for r in rows])
        n = len(returns)

        alpha = 1.0 - confidence

        # Historical VaR
        hist_var = self._historical_var(returns, alpha, horizon)
        hist_es = self._historical_es(returns, alpha, horizon)

        # Parametric VaR (normal)
        norm_var, norm_es = self._parametric_var_normal(returns, alpha, horizon)

        # Parametric VaR (Student-t)
        t_var, t_es, t_df = self._parametric_var_t(returns, alpha, horizon)

        # Monte Carlo VaR
        mc_var, mc_es = self._monte_carlo_var(returns, alpha, horizon, n_simulations)

        # Backtesting: Kupiec unconditional coverage test
        kupiec = self._kupiec_test(returns, hist_var, alpha)

        # Backtesting: Christoffersen conditional coverage test
        christoffersen = self._christoffersen_test(returns, hist_var, alpha)

        # Score: based on backtest results and VaR magnitude
        # More breaches than expected = higher score
        expected_breaches = alpha * (n - 1)
        actual_breaches = kupiec["n_violations"]
        breach_ratio = actual_breaches / max(expected_breaches, 1.0)
        breach_component = float(np.clip(breach_ratio * 40.0, 0, 100))

        # Large VaR magnitude = higher score
        var_pct = abs(hist_var) * 100  # as percentage
        var_component = float(np.clip(var_pct * 10.0, 0, 100))

        # Failed backtests = higher score
        test_component = 0.0
        if kupiec["reject"]:
            test_component += 25.0
        if christoffersen["reject"]:
            test_component += 25.0

        score = float(np.clip(
            0.40 * breach_component + 0.30 * var_component + 0.30 * test_component,
            0, 100,
        ))

        return {
            "score": round(score, 2),
            "country": country,
            "portfolio_id": portfolio_id,
            "confidence": confidence,
            "horizon_days": horizon,
            "n_obs": n,
            "historical": {
                "var": round(hist_var, 6),
                "es": round(hist_es, 6),
            },
            "parametric_normal": {
                "var": round(norm_var, 6),
                "es": round(norm_es, 6),
            },
            "parametric_t": {
                "var": round(t_var, 6),
                "es": round(t_es, 6),
                "degrees_of_freedom": round(t_df, 2),
            },
            "monte_carlo": {
                "var": round(mc_var, 6),
                "es": round(mc_es, 6),
                "n_simulations": n_simulations,
            },
            "backtesting": {
                "kupiec": {
                    "n_violations": kupiec["n_violations"],
                    "expected_violations": round(expected_breaches, 1),
                    "lr_statistic": round(kupiec["lr_stat"], 4),
                    "p_value": round(kupiec["p_value"], 4),
                    "reject": kupiec["reject"],
                },
                "christoffersen": {
                    "lr_statistic": round(christoffersen["lr_stat"], 4),
                    "p_value": round(christoffersen["p_value"], 4),
                    "reject": christoffersen["reject"],
                },
            },
            "summary_stats": {
                "mean_return": round(float(np.mean(returns)), 6),
                "std_return": round(float(np.std(returns, ddof=1)), 6),
                "skewness": round(float(sp_stats.skew(returns)), 4),
                "kurtosis": round(float(sp_stats.kurtosis(returns)), 4),
                "min_return": round(float(np.min(returns)), 6),
                "max_return": round(float(np.max(returns)), 6),
            },
        }

    @staticmethod
    def _historical_var(returns: np.ndarray, alpha: float, horizon: int) -> float:
        """Historical simulation VaR."""
        if horizon > 1:
            # Overlapping multi-day returns
            multi = np.array([np.sum(returns[i:i + horizon])
                              for i in range(len(returns) - horizon + 1)])
            return float(np.percentile(multi, alpha * 100))
        return float(np.percentile(returns, alpha * 100))

    @staticmethod
    def _historical_es(returns: np.ndarray, alpha: float, horizon: int) -> float:
        """Historical Expected Shortfall (CVaR)."""
        if horizon > 1:
            multi = np.array([np.sum(returns[i:i + horizon])
                              for i in range(len(returns) - horizon + 1)])
            var = np.percentile(multi, alpha * 100)
            tail = multi[multi <= var]
        else:
            var = np.percentile(returns, alpha * 100)
            tail = returns[returns <= var]
        return float(np.mean(tail)) if len(tail) > 0 else float(var)

    @staticmethod
    def _parametric_var_normal(returns: np.ndarray, alpha: float,
                               horizon: int) -> tuple[float, float]:
        """Parametric VaR assuming normal distribution."""
        mu = float(np.mean(returns))
        sigma = float(np.std(returns, ddof=1))
        z = sp_stats.norm.ppf(alpha)

        var = mu * horizon + z * sigma * np.sqrt(horizon)
        # ES for normal: mu - sigma * phi(z) / alpha
        es = mu * horizon - sigma * np.sqrt(horizon) * sp_stats.norm.pdf(z) / alpha

        return float(var), float(es)

    @staticmethod
    def _parametric_var_t(returns: np.ndarray, alpha: float,
                          horizon: int) -> tuple[float, float, float]:
        """Parametric VaR with Student-t distribution (fatter tails)."""
        # Fit Student-t
        df, loc, scale = sp_stats.t.fit(returns)
        df = max(df, 2.01)  # Ensure variance exists

        t_quantile = sp_stats.t.ppf(alpha, df)
        var = loc * horizon + t_quantile * scale * np.sqrt(horizon)

        # ES for Student-t
        es_factor = (sp_stats.t.pdf(t_quantile, df) * (df + t_quantile ** 2)
                     / ((df - 1) * alpha))
        es = loc * horizon - scale * np.sqrt(horizon) * es_factor

        return float(var), float(es), float(df)

    @staticmethod
    def _monte_carlo_var(returns: np.ndarray, alpha: float, horizon: int,
                         n_sims: int) -> tuple[float, float]:
        """Monte Carlo VaR via simulated return paths."""
        mu = float(np.mean(returns))
        sigma = float(np.std(returns, ddof=1))

        rng = np.random.default_rng(42)
        # Simulate daily returns and aggregate over horizon
        sims = rng.normal(mu, sigma, size=(n_sims, horizon))
        portfolio_returns = np.sum(sims, axis=1)

        var = float(np.percentile(portfolio_returns, alpha * 100))
        tail = portfolio_returns[portfolio_returns <= var]
        es = float(np.mean(tail)) if len(tail) > 0 else var

        return var, es

    @staticmethod
    def _kupiec_test(returns: np.ndarray, var: float, alpha: float) -> dict:
        """Kupiec (1995) unconditional coverage test for VaR backtesting.

        H0: violation rate = alpha.
        LR = -2 * ln[(1-alpha)^(T-N) * alpha^N] + 2 * ln[(1-N/T)^(T-N) * (N/T)^N]
        Under H0, LR ~ chi2(1).
        """
        violations = returns < var
        n_viol = int(np.sum(violations))
        T = len(returns)

        if n_viol == 0 or n_viol == T:
            return {"n_violations": n_viol, "lr_stat": 0.0, "p_value": 1.0, "reject": False}

        pi_hat = n_viol / T
        # Log-likelihood ratio
        lr = -2.0 * ((T - n_viol) * np.log(1 - alpha) + n_viol * np.log(alpha))
        lr += 2.0 * ((T - n_viol) * np.log(1 - pi_hat) + n_viol * np.log(pi_hat))

        p_value = float(1.0 - sp_stats.chi2.cdf(lr, df=1))

        return {
            "n_violations": n_viol,
            "lr_stat": float(lr),
            "p_value": p_value,
            "reject": p_value < 0.05,
        }

    @staticmethod
    def _christoffersen_test(returns: np.ndarray, var: float, alpha: float) -> dict:
        """Christoffersen (1998) conditional coverage test.

        Tests for both correct coverage AND independence of violations.
        LR_cc = LR_uc + LR_ind, under H0 ~ chi2(2).
        """
        violations = (returns < var).astype(int)
        T = len(violations)

        if T < 3:
            return {"lr_stat": 0.0, "p_value": 1.0, "reject": False}

        # Transition counts
        n00 = n01 = n10 = n11 = 0
        for i in range(1, T):
            prev, curr = violations[i - 1], violations[i]
            if prev == 0 and curr == 0:
                n00 += 1
            elif prev == 0 and curr == 1:
                n01 += 1
            elif prev == 1 and curr == 0:
                n10 += 1
            else:
                n11 += 1

        # Transition probabilities
        pi01 = n01 / max(n00 + n01, 1)
        pi11 = n11 / max(n10 + n11, 1)
        pi = (n01 + n11) / max(T - 1, 1)

        if pi <= 0 or pi >= 1 or pi01 <= 0 or pi01 >= 1:
            return {"lr_stat": 0.0, "p_value": 1.0, "reject": False}

        # Independence LR
        lr_ind = 0.0
        if n00 > 0 and (1 - pi) > 0 and (1 - pi01) > 0:
            lr_ind += n00 * np.log((1 - pi) / (1 - pi01))
        if n01 > 0 and pi > 0 and pi01 > 0:
            lr_ind += n01 * np.log(pi / pi01)
        if n10 > 0 and (1 - pi) > 0:
            p10 = 1 - pi11 if (n10 + n11) > 0 else (1 - pi)
            if p10 > 0:
                lr_ind += n10 * np.log((1 - pi) / p10)
        if n11 > 0 and pi > 0 and pi11 > 0:
            lr_ind += n11 * np.log(pi / pi11)
        lr_ind *= -2.0

        # Unconditional coverage LR
        n_viol = n01 + n11
        pi_hat = n_viol / max(T, 1)
        lr_uc = 0.0
        if n_viol > 0 and n_viol < T and alpha > 0 and alpha < 1:
            lr_uc = -2.0 * ((T - n_viol) * np.log(1 - alpha) + n_viol * np.log(alpha))
            lr_uc += 2.0 * ((T - n_viol) * np.log(1 - pi_hat) + n_viol * np.log(pi_hat))

        lr_cc = lr_uc + max(lr_ind, 0.0)
        p_value = float(1.0 - sp_stats.chi2.cdf(lr_cc, df=2))

        return {
            "lr_stat": float(lr_cc),
            "p_value": p_value,
            "reject": p_value < 0.05,
        }
