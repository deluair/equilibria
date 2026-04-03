"""Markowitz mean-variance portfolio optimization.

Computes the efficient frontier, minimum variance portfolio, and tangent
(maximum Sharpe) portfolio. Extends with Black-Litterman views to combine
equilibrium expected returns with investor views.

Score (0-100): based on portfolio Sharpe ratio and diversification ratio.
Low Sharpe or concentrated allocations push toward STRESS/CRISIS.
"""

from __future__ import annotations

import numpy as np
from scipy.optimize import minimize

from app.layers.base import LayerBase


class EfficientFrontier(LayerBase):
    layer_id = "l7"
    name = "Efficient Frontier"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        lookback = kwargs.get("lookback_years", 5)
        n_frontier = kwargs.get("n_frontier_points", 50)
        risk_free_rate = kwargs.get("risk_free_rate", 0.04)

        rows = await db.fetch_all(
            """
            SELECT ds.description, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('fred', 'yahoo', 'asset_returns')
              AND ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
            ORDER BY ds.description, dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no return data"}

        # Group by asset
        asset_data: dict[str, dict[str, float]] = {}
        for r in rows:
            desc = r["description"] or "unknown"
            asset_data.setdefault(desc, {})[r["date"]] = float(r["value"])

        asset_names = list(asset_data.keys())
        n_assets = len(asset_names)
        if n_assets < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "need >= 2 assets"}

        # Align dates
        all_dates = set.intersection(*(set(v.keys()) for v in asset_data.values()))
        dates = sorted(all_dates)
        if len(dates) < 12:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient overlapping dates"}

        returns = np.array([
            [asset_data[a][d] for d in dates] for a in asset_names
        ])  # shape: (n_assets, T)

        mu = np.mean(returns, axis=1)  # annualized expected returns
        cov = np.cov(returns)
        if cov.ndim == 0:
            cov = np.array([[float(cov)]])

        # Minimum variance portfolio
        min_var = self._min_variance_portfolio(mu, cov, n_assets)

        # Tangent (max Sharpe) portfolio
        tangent = self._tangent_portfolio(mu, cov, n_assets, risk_free_rate)

        # Efficient frontier
        frontier = self._compute_frontier(mu, cov, n_assets, n_frontier, risk_free_rate)

        # Diversification ratio: weighted avg vol / portfolio vol
        if tangent["vol"] > 1e-10:
            asset_vols = np.sqrt(np.diag(cov))
            div_ratio = float(tangent["weights"] @ asset_vols / tangent["vol"])
        else:
            div_ratio = 1.0

        # Black-Litterman (if views provided)
        views = kwargs.get("views")  # list of dicts: {asset_idx, view_return, confidence}
        bl_result = None
        if views and len(views) > 0:
            bl_result = self._black_litterman(mu, cov, n_assets, risk_free_rate, views)

        # Score: low Sharpe = high stress, low diversification = high stress
        sharpe = tangent["sharpe"]
        sharpe_component = float(np.clip(50.0 - sharpe * 25.0, 0, 100))
        div_component = float(np.clip((2.0 - div_ratio) * 50.0, 0, 100))
        score = float(np.clip(0.60 * sharpe_component + 0.40 * div_component, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_assets": n_assets,
            "n_obs": len(dates),
            "asset_names": asset_names,
            "expected_returns": [round(float(m), 6) for m in mu],
            "volatilities": [round(float(np.sqrt(cov[i, i])), 6) for i in range(n_assets)],
            "correlation_matrix": np.corrcoef(returns).round(4).tolist(),
            "min_variance_portfolio": {
                "weights": {asset_names[i]: round(float(min_var["weights"][i]), 4)
                            for i in range(n_assets)},
                "return": round(min_var["ret"], 6),
                "volatility": round(min_var["vol"], 6),
                "sharpe": round(min_var["sharpe"], 4),
            },
            "tangent_portfolio": {
                "weights": {asset_names[i]: round(float(tangent["weights"][i]), 4)
                            for i in range(n_assets)},
                "return": round(tangent["ret"], 6),
                "volatility": round(tangent["vol"], 6),
                "sharpe": round(tangent["sharpe"], 4),
            },
            "diversification_ratio": round(div_ratio, 4),
            "efficient_frontier": frontier,
            "black_litterman": bl_result,
        }

    @staticmethod
    def _min_variance_portfolio(mu: np.ndarray, cov: np.ndarray, n: int) -> dict:
        """Global minimum variance portfolio (long-only)."""
        def objective(w):
            return float(w @ cov @ w)

        constraints = [{"type": "eq", "fun": lambda w: np.sum(w) - 1.0}]
        bounds = [(0.0, 1.0)] * n
        w0 = np.ones(n) / n

        result = minimize(objective, w0, method="SLSQP", bounds=bounds, constraints=constraints)
        w = result.x if result.success else w0
        ret = float(w @ mu)
        vol = float(np.sqrt(w @ cov @ w))
        sharpe = ret / vol if vol > 1e-10 else 0.0

        return {"weights": w, "ret": ret, "vol": vol, "sharpe": sharpe}

    @staticmethod
    def _tangent_portfolio(mu: np.ndarray, cov: np.ndarray, n: int, rf: float) -> dict:
        """Maximum Sharpe ratio portfolio (long-only)."""
        def neg_sharpe(w):
            ret = float(w @ mu)
            vol = float(np.sqrt(w @ cov @ w))
            return -(ret - rf) / vol if vol > 1e-10 else 0.0

        constraints = [{"type": "eq", "fun": lambda w: np.sum(w) - 1.0}]
        bounds = [(0.0, 1.0)] * n
        w0 = np.ones(n) / n

        result = minimize(neg_sharpe, w0, method="SLSQP", bounds=bounds, constraints=constraints)
        w = result.x if result.success else w0
        ret = float(w @ mu)
        vol = float(np.sqrt(w @ cov @ w))
        sharpe = (ret - rf) / vol if vol > 1e-10 else 0.0

        return {"weights": w, "ret": ret, "vol": vol, "sharpe": sharpe}

    @staticmethod
    def _compute_frontier(mu: np.ndarray, cov: np.ndarray, n: int,
                          n_points: int, rf: float) -> list[dict]:
        """Trace efficient frontier by minimizing variance at target returns."""
        min_ret = float(np.min(mu))
        max_ret = float(np.max(mu))
        targets = np.linspace(min_ret, max_ret, n_points)
        frontier = []

        for target in targets:
            def objective(w):
                return float(w @ cov @ w)

            constraints = [
                {"type": "eq", "fun": lambda w: np.sum(w) - 1.0},
                {"type": "eq", "fun": lambda w, t=target: float(w @ mu) - t},
            ]
            bounds = [(0.0, 1.0)] * n
            w0 = np.ones(n) / n

            result = minimize(objective, w0, method="SLSQP", bounds=bounds,
                              constraints=constraints, options={"maxiter": 200})
            if result.success:
                vol = float(np.sqrt(result.fun))
                sharpe = (target - rf) / vol if vol > 1e-10 else 0.0
                frontier.append({
                    "return": round(target, 6),
                    "volatility": round(vol, 6),
                    "sharpe": round(sharpe, 4),
                })

        return frontier

    @staticmethod
    def _black_litterman(mu: np.ndarray, cov: np.ndarray, n: int,
                         rf: float, views: list[dict]) -> dict:
        """Black-Litterman model combining equilibrium returns with investor views.

        views: list of {asset_idx: int, view_return: float, confidence: float (0-1)}
        """
        # Implied equilibrium returns (reverse optimization from market cap weights)
        # Assume equal weights as proxy for market cap weights
        delta = 2.5  # risk aversion coefficient
        w_eq = np.ones(n) / n
        pi = delta * cov @ w_eq  # equilibrium excess returns

        # Build P (pick matrix) and Q (view returns) and Omega (uncertainty)
        k = len(views)
        P = np.zeros((k, n))
        Q = np.zeros(k)
        tau = 0.05  # scaling factor for uncertainty of equilibrium returns

        view_confidences = []
        for i, v in enumerate(views):
            idx = v.get("asset_idx", 0)
            if 0 <= idx < n:
                P[i, idx] = 1.0
                Q[i] = v.get("view_return", 0.0)
                view_confidences.append(v.get("confidence", 0.5))

        # Omega: diagonal uncertainty of views (inversely related to confidence)
        omega_diag = []
        for conf in view_confidences:
            c = max(0.01, min(0.99, conf))
            omega_diag.append(tau * (1.0 - c) / c)
        Omega = np.diag(omega_diag)

        # BL posterior expected returns
        tau_cov = tau * cov
        tau_cov_inv = np.linalg.inv(tau_cov)
        Omega_inv = np.linalg.inv(Omega)
        M = np.linalg.inv(tau_cov_inv + P.T @ Omega_inv @ P)
        bl_mu = M @ (tau_cov_inv @ pi + P.T @ Omega_inv @ Q)

        return {
            "equilibrium_returns": [round(float(p), 6) for p in pi],
            "posterior_returns": [round(float(b), 6) for b in bl_mu],
            "n_views": k,
        }
