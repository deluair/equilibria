"""Money Demand estimation module.

Methodology
-----------
Baumol (1952) and Tobin (1956) inventory model of money demand:

    M*/P = sqrt(Y * F / (2 * i))

where:
    M*/P = optimal real money balances
    Y    = real income (transactions volume)
    F    = fixed cost per portfolio adjustment
    i    = nominal interest rate (opportunity cost)

The income elasticity is 0.5 and the interest elasticity is -0.5 under
strict Baumol-Tobin. Empirical estimates typically find income elasticity
closer to 1.0 (Goldfeld, 1973) and interest elasticity between -0.1 and -0.5.

Money demand function:

    ln(M/P) = beta_0 + beta_1 * ln(Y) + beta_2 * i + e

Velocity decomposition:

    V = PY / M  =>  ln(V) = ln(Y) - ln(M/P)

Financial innovation effects are captured by trend terms and proxy
variables (ATM density, electronic payments share) that shift the
demand function downward over time.

Score reflects demand function stability (low = stable, high = stress).

Sources: FRED (M1SL, M2SL, GDP, FEDFUNDS, CPI)
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class MoneyDemand(LayerBase):
    layer_id = "l15"
    name = "Money Demand"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        aggregate = kwargs.get("aggregate", "M2")
        lookback = kwargs.get("lookback_years", 30)

        series_map = {
            "money": f"{aggregate}_{country}",
            "gdp": f"RGDP_{country}",
            "price_level": f"CPI_{country}",
            "interest_rate": f"POLICY_RATE_{country}",
        }

        data: dict[str, dict[str, float]] = {}
        for label, code in series_map.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE series_id = ?) "
                "AND date >= date('now', ?) ORDER BY date",
                (code, f"-{lookback} years"),
            )
            if rows:
                data[label] = {r[0]: float(r[1]) for r in rows}

        if not data.get("money") or not data.get("gdp"):
            return {"score": 50.0, "results": {"error": "insufficient data"}}

        common = sorted(
            set(data["money"])
            & set(data["gdp"])
            & set(data.get("price_level", data["money"]))
            & set(data.get("interest_rate", data["money"]))
        )
        if len(common) < 12:
            return {"score": 50.0, "results": {"error": "too few overlapping observations"}}

        M = np.array([data["money"][d] for d in common])
        Y = np.array([data["gdp"][d] for d in common])
        P = (
            np.array([data["price_level"][d] for d in common])
            if data.get("price_level")
            else np.ones(len(common))
        )
        i_rate = (
            np.array([data["interest_rate"][d] for d in common])
            if data.get("interest_rate")
            else np.full(len(common), 2.0)
        )

        # Real money balances
        real_M = M / P
        real_Y = Y

        results: dict = {
            "country": country,
            "aggregate": aggregate,
            "n_obs": len(common),
            "period": f"{common[0]} to {common[-1]}",
        }

        # --- 1. Baumol-Tobin implied elasticities ---
        results["baumol_tobin"] = {
            "theoretical_income_elasticity": 0.5,
            "theoretical_interest_elasticity": -0.5,
        }

        # --- 2. Empirical money demand function ---
        # ln(M/P) = b0 + b1*ln(Y) + b2*i + b3*t + e
        ln_real_M = np.log(np.maximum(real_M, 1e-10))
        ln_Y = np.log(np.maximum(real_Y, 1e-10))
        t = np.arange(len(common), dtype=float)

        X = np.column_stack([np.ones(len(common)), ln_Y, i_rate, t])
        beta, residuals, rank, sv = np.linalg.lstsq(X, ln_real_M, rcond=None)

        fitted = X @ beta
        resid = ln_real_M - fitted
        n = len(common)
        k = X.shape[1]
        sse = float(np.sum(resid ** 2))
        sst = float(np.sum((ln_real_M - np.mean(ln_real_M)) ** 2))
        r_squared = 1.0 - sse / sst if sst > 0 else 0.0

        # Robust HC1 standard errors
        bread = np.linalg.inv(X.T @ X)
        meat = X.T @ np.diag(resid ** 2) @ X
        vcov = (n / max(n - k, 1)) * bread @ meat @ bread
        se = np.sqrt(np.diag(vcov))

        income_elasticity = float(beta[1])
        interest_semi_elasticity = float(beta[2])
        trend_coef = float(beta[3])

        results["demand_function"] = {
            "income_elasticity": round(income_elasticity, 4),
            "income_elasticity_se": round(float(se[1]), 4),
            "interest_semi_elasticity": round(interest_semi_elasticity, 4),
            "interest_semi_elasticity_se": round(float(se[2]), 4),
            "trend_coefficient": round(trend_coef, 6),
            "trend_se": round(float(se[3]), 6),
            "r_squared": round(r_squared, 4),
            "baumol_tobin_consistent": 0.3 <= income_elasticity <= 0.7,
        }

        # --- 3. Velocity decomposition ---
        velocity = (P * Y) / np.maximum(M, 1e-10)
        v_mean = float(np.mean(velocity))
        v_trend = np.polyfit(t, velocity, 1)
        v_growth_rate = float(v_trend[0] / v_mean * 100) if v_mean > 0 else 0.0

        results["velocity"] = {
            "current": round(float(velocity[-1]), 4),
            "mean": round(v_mean, 4),
            "std": round(float(np.std(velocity, ddof=1)), 4),
            "trend_slope": round(float(v_trend[0]), 6),
            "annualized_growth_pct": round(v_growth_rate, 2),
            "declining": v_trend[0] < 0,
        }

        # --- 4. Financial innovation proxy (trend in residuals) ---
        # Positive trend in residuals = unexplained money growth
        resid_trend = np.polyfit(t, resid, 1)
        # Structural break test: Chow test at midpoint
        mid = n // 2
        if mid > k + 2 and (n - mid) > k + 2:
            sse_1 = float(np.sum(
                (ln_real_M[:mid] - X[:mid] @ np.linalg.lstsq(X[:mid], ln_real_M[:mid], rcond=None)[0]) ** 2
            ))
            sse_2 = float(np.sum(
                (ln_real_M[mid:] - X[mid:] @ np.linalg.lstsq(X[mid:], ln_real_M[mid:], rcond=None)[0]) ** 2
            ))
            chow_f = ((sse - sse_1 - sse_2) / k) / ((sse_1 + sse_2) / max(n - 2 * k, 1))
            chow_p = 1.0 - sp_stats.f.cdf(abs(chow_f), k, max(n - 2 * k, 1))
        else:
            chow_f = None
            chow_p = None

        results["financial_innovation"] = {
            "residual_trend_slope": round(float(resid_trend[0]), 6),
            "residual_drift_detected": abs(resid_trend[0]) > 0.005,
            "chow_break_f_stat": round(chow_f, 3) if chow_f is not None else None,
            "chow_break_p_value": round(chow_p, 4) if chow_p is not None else None,
            "structural_break_detected": chow_p < 0.05 if chow_p is not None else None,
        }

        # --- 5. Demand stability (CUSUM-like) ---
        cusum = np.cumsum(resid / np.std(resid, ddof=1)) if np.std(resid, ddof=1) > 0 else np.zeros(n)
        cusum_max = float(np.max(np.abs(cusum)))
        # Critical value approximation: 1.36 * sqrt(n) at 5%
        cusum_critical = 1.36 * np.sqrt(n)
        stability_rejected = cusum_max > cusum_critical

        results["stability"] = {
            "cusum_max": round(cusum_max, 3),
            "cusum_critical_5pct": round(float(cusum_critical), 3),
            "stable": not stability_rejected,
        }

        # --- Score ---
        # Instability -> stress
        fit_penalty = (1.0 - max(r_squared, 0)) * 20
        stability_penalty = 25.0 if stability_rejected else 0.0
        break_penalty = 15.0 if results["financial_innovation"].get("structural_break_detected") else 0.0
        velocity_penalty = min(abs(v_growth_rate) * 3, 20)
        residual_drift_penalty = 10.0 if results["financial_innovation"]["residual_drift_detected"] else 0.0

        score = min(fit_penalty + stability_penalty + break_penalty + velocity_penalty + residual_drift_penalty, 100)

        return {"score": round(score, 1), "results": results}
