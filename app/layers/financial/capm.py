"""Capital Asset Pricing Model estimation.

Estimates systematic risk (beta), Jensen's alpha, and Treynor ratio from asset
return series regressed on market excess returns. Extends to Fama-French 3-factor
(market, SMB, HML) and 5-factor (+ RMW, CMA) models.

Score (0-100): based on alpha significance and model stability. Large negative
alpha or unstable beta signals mispricing or elevated systematic risk.
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class CAPM(LayerBase):
    layer_id = "l7"
    name = "CAPM"

    async def compute(self, db, **kwargs) -> dict:
        asset_id = kwargs.get("asset_id", "market_index")
        country = kwargs.get("country_iso3", "USA")
        lookback = kwargs.get("lookback_years", 5)

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.description
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('fred', 'yahoo', 'factor_returns')
              AND ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
            ORDER BY dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not rows or len(rows) < 24:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient return data"}

        # Parse returns into series: asset, market, rf, SMB, HML, RMW, CMA
        series: dict[str, dict[str, float]] = {}
        for r in rows:
            desc = (r["description"] or "").lower()
            key = self._classify_series(desc, asset_id)
            if key:
                series.setdefault(key, {})[r["date"]] = float(r["value"])

        # Need at least asset returns and market returns
        if "asset" not in series or "market" not in series:
            return {"score": None, "signal": "UNAVAILABLE", "error": "missing asset or market series"}

        # Align dates
        common_dates = sorted(set(series["asset"]) & set(series["market"]))
        if len(common_dates) < 24:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient overlapping dates"}

        r_asset = np.array([series["asset"][d] for d in common_dates])
        r_market = np.array([series["market"][d] for d in common_dates])
        r_rf = np.array([series.get("rf", {}).get(d, 0.0) for d in common_dates])

        # Excess returns
        excess_asset = r_asset - r_rf
        excess_market = r_market - r_rf

        # Single-factor CAPM
        capm_result = self._ols_regression(excess_market, excess_asset)

        # Treynor ratio
        beta = capm_result["beta"]
        mean_excess = float(np.mean(excess_asset))
        treynor = mean_excess / beta if abs(beta) > 1e-6 else None

        # Fama-French 3-factor
        ff3_result = None
        if all(k in series for k in ("smb", "hml")):
            ff3_dates = sorted(set(common_dates) & set(series["smb"]) & set(series["hml"]))
            if len(ff3_dates) >= 24:
                y_ff3 = np.array([series["asset"][d] - series.get("rf", {}).get(d, 0.0)
                                  for d in ff3_dates])
                X_ff3 = np.column_stack([
                    np.array([series["market"][d] - series.get("rf", {}).get(d, 0.0)
                              for d in ff3_dates]),
                    np.array([series["smb"][d] for d in ff3_dates]),
                    np.array([series["hml"][d] for d in ff3_dates]),
                ])
                ff3_result = self._multi_factor_ols(X_ff3, y_ff3,
                                                    ["market", "smb", "hml"])

        # Fama-French 5-factor
        ff5_result = None
        if all(k in series for k in ("smb", "hml", "rmw", "cma")):
            ff5_dates = sorted(
                set(common_dates) & set(series["smb"]) & set(series["hml"])
                & set(series["rmw"]) & set(series["cma"])
            )
            if len(ff5_dates) >= 36:
                y_ff5 = np.array([series["asset"][d] - series.get("rf", {}).get(d, 0.0)
                                  for d in ff5_dates])
                X_ff5 = np.column_stack([
                    np.array([series["market"][d] - series.get("rf", {}).get(d, 0.0)
                              for d in ff5_dates]),
                    np.array([series["smb"][d] for d in ff5_dates]),
                    np.array([series["hml"][d] for d in ff5_dates]),
                    np.array([series["rmw"][d] for d in ff5_dates]),
                    np.array([series["cma"][d] for d in ff5_dates]),
                ])
                ff5_result = self._multi_factor_ols(X_ff5, y_ff5,
                                                    ["market", "smb", "hml", "rmw", "cma"])

        # Score: large negative alpha or high beta instability = stress
        alpha = capm_result["alpha"]
        alpha_t = capm_result["alpha_t"]
        # Significant negative alpha: high score. Significant positive: low score.
        alpha_component = float(np.clip(50.0 - alpha_t * 10.0, 0, 100))
        # High beta (>1.5) = high systematic risk
        beta_component = float(np.clip((abs(beta) - 1.0) * 50.0, 0, 100))
        # Low R2 = poor model fit = uncertainty
        r2_component = float(np.clip((1.0 - capm_result["r_squared"]) * 50.0, 0, 100))

        score = float(np.clip(
            0.50 * alpha_component + 0.30 * beta_component + 0.20 * r2_component,
            0, 100,
        ))

        return {
            "score": round(score, 2),
            "country": country,
            "asset_id": asset_id,
            "n_obs": len(common_dates),
            "capm": {
                "alpha": round(alpha, 6),
                "alpha_t": round(alpha_t, 3),
                "alpha_p": round(capm_result["alpha_p"], 4),
                "beta": round(beta, 4),
                "beta_t": round(capm_result["beta_t"], 3),
                "beta_se": round(capm_result["beta_se"], 4),
                "r_squared": round(capm_result["r_squared"], 4),
                "treynor_ratio": round(treynor, 6) if treynor is not None else None,
                "mean_excess_return": round(mean_excess, 6),
                "residual_vol": round(float(np.std(capm_result["residuals"])), 6),
            },
            "fama_french_3": ff3_result,
            "fama_french_5": ff5_result,
        }

    @staticmethod
    def _classify_series(desc: str, asset_id: str) -> str | None:
        if asset_id.lower() in desc or "asset_return" in desc:
            return "asset"
        if "market_return" in desc or "sp500" in desc or "mkt" in desc:
            return "market"
        if "risk_free" in desc or "tbill" in desc or "rf" in desc:
            return "rf"
        if "smb" in desc:
            return "smb"
        if "hml" in desc:
            return "hml"
        if "rmw" in desc:
            return "rmw"
        if "cma" in desc:
            return "cma"
        return None

    @staticmethod
    def _ols_regression(x: np.ndarray, y: np.ndarray) -> dict:
        """Single-factor OLS: y = alpha + beta * x + epsilon."""
        n = len(x)
        X = np.column_stack([np.ones(n), x])
        beta_hat = np.linalg.lstsq(X, y, rcond=None)[0]
        residuals = y - X @ beta_hat
        ss_res = float(np.sum(residuals ** 2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        # HC1 robust standard errors
        XtX_inv = np.linalg.inv(X.T @ X)
        omega = np.diag(residuals ** 2) * (n / (n - 2))
        V = XtX_inv @ (X.T @ omega @ X) @ XtX_inv
        se = np.sqrt(np.diag(V))

        alpha_t = float(beta_hat[0] / se[0]) if se[0] > 1e-10 else 0.0
        beta_t = float(beta_hat[1] / se[1]) if se[1] > 1e-10 else 0.0
        alpha_p = float(2.0 * (1.0 - sp_stats.t.cdf(abs(alpha_t), df=n - 2)))
        beta_p = float(2.0 * (1.0 - sp_stats.t.cdf(abs(beta_t), df=n - 2)))

        return {
            "alpha": float(beta_hat[0]),
            "beta": float(beta_hat[1]),
            "alpha_t": alpha_t,
            "beta_t": beta_t,
            "alpha_p": alpha_p,
            "beta_p": beta_p,
            "alpha_se": float(se[0]),
            "beta_se": float(se[1]),
            "r_squared": max(0.0, r2),
            "residuals": residuals,
        }

    @staticmethod
    def _multi_factor_ols(X_factors: np.ndarray, y: np.ndarray,
                          factor_names: list[str]) -> dict:
        """Multi-factor OLS with HC1 robust SE."""
        n, k = X_factors.shape
        X = np.column_stack([np.ones(n), X_factors])
        beta_hat = np.linalg.lstsq(X, y, rcond=None)[0]
        residuals = y - X @ beta_hat
        ss_res = float(np.sum(residuals ** 2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        XtX_inv = np.linalg.inv(X.T @ X)
        omega = np.diag(residuals ** 2) * (n / (n - k - 1))
        V = XtX_inv @ (X.T @ omega @ X) @ XtX_inv
        se = np.sqrt(np.maximum(np.diag(V), 0.0))

        names = ["alpha"] + factor_names
        coefficients = {}
        t_stats = {}
        for i, nm in enumerate(names):
            coefficients[nm] = round(float(beta_hat[i]), 6)
            t_stats[nm] = round(float(beta_hat[i] / se[i]), 3) if se[i] > 1e-10 else 0.0

        return {
            "coefficients": coefficients,
            "t_stats": t_stats,
            "r_squared": round(max(0.0, r2), 4),
            "n_obs": n,
        }
