"""Credit risk modeling.

Implements the Merton (1974) structural model for corporate default, KMV
distance-to-default, CreditMetrics-style transition matrix estimation, and
PD/LGD/EAD estimation for expected loss calculation.

Score (0-100): based on distance-to-default and expected loss. Short DD or
high expected loss signals elevated credit risk.
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats
from scipy.optimize import brentq

from app.layers.base import LayerBase


class CreditRisk(LayerBase):
    layer_id = "l7"
    name = "Credit Risk"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        lookback = kwargs.get("lookback_years", 5)

        rows = await db.fetch_all(
            """
            SELECT ds.description, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('fred', 'credit', 'corporate')
              AND ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
            ORDER BY ds.description, dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no credit data"}

        # Parse into series
        series: dict[str, list[float]] = {}
        for r in rows:
            desc = (r["description"] or "").lower()
            series.setdefault(desc, []).append(float(r["value"]))

        # Try to extract key variables
        equity_values = self._find_series(series, ["equity", "market_cap", "stock_price"])
        debt_values = self._find_series(series, ["debt", "liabilities", "face_value"])
        risk_free = self._find_series(series, ["risk_free", "rf", "tbill"])

        # Merton model
        merton_result = None
        if equity_values and debt_values:
            E = equity_values[-1]
            D = debt_values[-1]
            sigma_e = float(np.std(np.diff(np.log(np.maximum(equity_values, 1e-10))),
                                   ddof=1) * np.sqrt(252)) if len(equity_values) > 10 else 0.30
            r = risk_free[-1] if risk_free else 0.04
            T_mat = kwargs.get("maturity_years", 1.0)

            merton_result = self._merton_model(E, D, sigma_e, r, T_mat)

        # KMV distance-to-default
        dd_result = None
        if merton_result:
            dd_result = {
                "distance_to_default": merton_result["dd"],
                "default_probability": merton_result["pd"],
                "asset_value": merton_result["V"],
                "asset_volatility": merton_result["sigma_v"],
            }

        # CreditMetrics transition matrix
        transition_matrix = None
        ratings = self._find_series(series, ["rating", "credit_score", "grade"])
        if ratings and len(ratings) > 12:
            transition_matrix = self._estimate_transition_matrix(ratings)

        # PD/LGD/EAD expected loss
        pd = merton_result["pd"] if merton_result else kwargs.get("pd", 0.02)
        lgd = kwargs.get("lgd", 0.45)  # Basel II foundation IRB default
        ead = kwargs.get("ead", debt_values[-1] if debt_values else 1.0)
        expected_loss = pd * lgd * ead

        # Credit spread from DD
        credit_spread = None
        if merton_result and merton_result["pd"] > 0:
            T_mat = kwargs.get("maturity_years", 1.0)
            r = risk_free[-1] if risk_free else 0.04
            credit_spread = self._implied_credit_spread(merton_result["pd"], lgd, r, T_mat)

        # Score: short DD = high risk, high PD = high risk
        dd = merton_result["dd"] if merton_result else 3.0
        pd_val = merton_result["pd"] if merton_result else pd

        # DD < 1 = crisis territory, DD > 4 = safe
        dd_component = float(np.clip((4.0 - dd) * 25.0, 0, 100))
        # PD > 5% = crisis, PD < 0.1% = safe
        pd_component = float(np.clip(pd_val * 1000.0, 0, 100))

        score = float(np.clip(0.60 * dd_component + 0.40 * pd_component, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "merton_model": {
                "asset_value": round(merton_result["V"], 2),
                "asset_volatility": round(merton_result["sigma_v"], 4),
                "distance_to_default": round(merton_result["dd"], 4),
                "default_probability": round(merton_result["pd"], 6),
                "d1": round(merton_result["d1"], 4),
                "d2": round(merton_result["d2"], 4),
            } if merton_result else None,
            "kmv": {
                "distance_to_default": round(dd_result["distance_to_default"], 4),
                "edf": round(dd_result["default_probability"], 6),
            } if dd_result else None,
            "expected_loss": {
                "pd": round(pd_val, 6),
                "lgd": round(lgd, 4),
                "ead": round(ead, 2),
                "el": round(expected_loss, 4),
            },
            "credit_spread_bps": round(credit_spread * 10000, 2) if credit_spread else None,
            "transition_matrix": transition_matrix,
        }

    @staticmethod
    def _find_series(series: dict[str, list[float]], keywords: list[str]) -> list[float] | None:
        for key, vals in series.items():
            for kw in keywords:
                if kw in key:
                    return vals
        return None

    @staticmethod
    def _merton_model(E: float, D: float, sigma_e: float, r: float,
                      T: float) -> dict:
        """Merton (1974) structural model.

        Solves for firm asset value V and asset volatility sigma_v from
        observed equity value E and equity volatility sigma_e, using the
        system:
            E = V * N(d1) - D * exp(-rT) * N(d2)
            sigma_e = (V / E) * N(d1) * sigma_v
        """
        # Initial guess: V = E + D, sigma_v = sigma_e * E / (E + D)
        V_init = max(E + D, D * 1.01)
        sigma_v_init = sigma_e * E / max(V_init, 1e-10)

        # Iterative solution (Vassalou-Xing approach)
        V = V_init
        sigma_v = max(sigma_v_init, 0.01)

        for _ in range(100):
            d1 = (np.log(V / D) + (r + 0.5 * sigma_v ** 2) * T) / (sigma_v * np.sqrt(T))
            d2 = d1 - sigma_v * np.sqrt(T)

            # Update V from E = V*N(d1) - D*exp(-rT)*N(d2)
            nd1 = sp_stats.norm.cdf(d1)

            try:
                def f(v):
                    d1_v = (np.log(v / D) + (r + 0.5 * sigma_v ** 2) * T) / (sigma_v * np.sqrt(T))
                    d2_v = d1_v - sigma_v * np.sqrt(T)
                    return v * sp_stats.norm.cdf(d1_v) - D * np.exp(-r * T) * sp_stats.norm.cdf(d2_v) - E

                V_new = brentq(f, D * 0.01, (E + D) * 10)
            except (ValueError, RuntimeError):
                V_new = V

            # Update sigma_v from sigma_e = (V/E) * N(d1) * sigma_v
            if nd1 > 1e-10 and E > 1e-10:
                sigma_v_new = sigma_e * E / (V_new * nd1)
            else:
                sigma_v_new = sigma_v

            if abs(V_new - V) / max(V, 1e-10) < 1e-6 and abs(sigma_v_new - sigma_v) < 1e-6:
                V = V_new
                sigma_v = sigma_v_new
                break
            V = V_new
            sigma_v = max(sigma_v_new, 0.01)

        d1 = (np.log(V / D) + (r + 0.5 * sigma_v ** 2) * T) / (sigma_v * np.sqrt(T))
        d2 = d1 - sigma_v * np.sqrt(T)
        dd = d2  # KMV distance-to-default
        pd = sp_stats.norm.cdf(-dd)

        return {
            "V": float(V),
            "sigma_v": float(sigma_v),
            "d1": float(d1),
            "d2": float(d2),
            "dd": float(dd),
            "pd": float(pd),
        }

    @staticmethod
    def _estimate_transition_matrix(ratings: list[float], n_states: int = 8) -> dict:
        """Estimate credit rating transition matrix from observed rating history.

        Discretizes continuous ratings into n_states buckets (AAA=0..D=7).
        """
        arr = np.array(ratings)
        # Discretize into rating buckets
        percentiles = np.linspace(0, 100, n_states + 1)
        bins = np.percentile(arr, percentiles)
        bins[-1] += 1  # Include max
        states = np.digitize(arr, bins[1:])
        states = np.clip(states, 0, n_states - 1)

        # Count transitions
        trans = np.zeros((n_states, n_states))
        for i in range(len(states) - 1):
            trans[states[i], states[i + 1]] += 1

        # Normalize rows
        row_sums = trans.sum(axis=1, keepdims=True)
        row_sums = np.maximum(row_sums, 1.0)
        trans_prob = trans / row_sums

        labels = ["AAA", "AA", "A", "BBB", "BB", "B", "CCC", "D"][:n_states]

        return {
            "states": labels,
            "matrix": trans_prob.round(4).tolist(),
        }

    @staticmethod
    def _implied_credit_spread(pd: float, lgd: float, r: float, T: float) -> float:
        """Implied credit spread from PD and LGD under risk-neutral pricing.

        spread = -(1/T) * ln(1 - pd * lgd) approximately = pd * lgd for small values.
        """
        loss = pd * lgd
        if loss >= 1.0:
            return 0.10  # Cap at 1000 bps
        return -np.log(1.0 - loss) / max(T, 0.01)
