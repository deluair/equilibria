"""Inflation Expectations module.

Methodology
-----------
Uses inflation persistence as a proxy for anchoring of inflation expectations.
A high AR(1) coefficient indicates that past inflation strongly predicts current
inflation, implying adaptive expectations and poor central bank credibility.

**Persistence estimation** (Pivetta & Reis 2007, Stock & Watson 2007):
    pi_t = mu + rho * pi_{t-1} + e_t

    - rho close to 1: unit-root-like persistence, unanchored expectations
    - rho < 0.5: well-anchored, inflation mean-reverts quickly

**Rolling persistence** tests whether anchoring has changed over time.
A structural break in rho signals a regime change (e.g., inflation targeting adoption).

**Mean reversion time**: tau = -1 / log(rho) periods for inflation to halve.

Score: high AR(1) coefficient -> high persistence -> unanchored -> stress.

Sources: WDI (FP.CPI.TOTL.ZG), FRED
"""

import numpy as np

from app.layers.base import LayerBase


def _ar1_ols(y: np.ndarray) -> dict:
    """Estimate AR(1) with HC1 standard errors. Returns rho, se, t_stat, r_squared."""
    n = len(y)
    y_lag = y[:-1]
    y_curr = y[1:]
    T = n - 1

    X = np.column_stack([np.ones(T), y_lag])
    beta = np.linalg.lstsq(X, y_curr, rcond=None)[0]
    resid = y_curr - X @ beta
    sse = float(np.sum(resid ** 2))
    sst = float(np.sum((y_curr - np.mean(y_curr)) ** 2))
    r_squared = 1 - sse / sst if sst > 0 else 0.0

    bread = np.linalg.inv(X.T @ X)
    meat = X.T @ np.diag(resid ** 2) @ X
    vcov = (T / (T - 2)) * bread @ meat @ bread
    se = np.sqrt(np.diag(vcov))

    rho = float(beta[1])
    rho_se = float(se[1])
    t_stat = rho / rho_se if rho_se > 1e-12 else 0.0

    return {
        "rho": rho,
        "const": float(beta[0]),
        "rho_se": rho_se,
        "t_stat": t_stat,
        "r_squared": r_squared,
        "n": T,
    }


class InflationExpectations(LayerBase):
    layer_id = "l2"
    name = "Inflation Expectations"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        rolling_window = kwargs.get("rolling_window", 20)

        rows = await db.execute_fetchall(
            "SELECT date, value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
            (f"INFLATION_{country}",),
        )

        if not rows or len(rows) < 10:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"insufficient inflation data for {country}",
            }

        dates = [r[0] for r in rows]
        pi = np.array([float(r[1]) for r in rows])
        n = len(pi)

        results = {
            "country": country,
            "n_obs": n,
            "period": f"{dates[0]} to {dates[-1]}",
        }

        # --- Full-sample AR(1) ---
        ar1 = _ar1_ols(pi)
        rho = ar1["rho"]
        results["ar1"] = {
            "rho": round(rho, 4),
            "rho_se": round(ar1["rho_se"], 4),
            "t_stat": round(ar1["t_stat"], 3),
            "r_squared": round(ar1["r_squared"], 4),
            "n": ar1["n"],
            "interpretation": (
                "High persistence (unanchored)" if rho > 0.7
                else "Moderate persistence" if rho > 0.4
                else "Low persistence (well-anchored)"
            ),
        }

        # Mean reversion half-life
        if rho > 0 and rho < 1:
            half_life = -np.log(2) / np.log(rho)
            results["ar1"]["half_life_periods"] = round(float(half_life), 1)
        elif rho >= 1:
            results["ar1"]["half_life_periods"] = None
            results["ar1"]["unit_root_warning"] = True
        else:
            results["ar1"]["half_life_periods"] = None

        # --- Rolling AR(1) for time-varying persistence ---
        if n >= rolling_window + 5:
            rolling_rho = []
            rolling_dates_out = []
            for i in range(n - rolling_window):
                window_data = pi[i : i + rolling_window]
                if len(window_data) >= 5:
                    ar1_roll = _ar1_ols(window_data)
                    rolling_rho.append(round(ar1_roll["rho"], 4))
                    rolling_dates_out.append(dates[i + rolling_window - 1])

            if rolling_rho:
                results["rolling_persistence"] = {
                    "window": rolling_window,
                    "values": rolling_rho,
                    "dates": rolling_dates_out,
                    "current": rolling_rho[-1],
                    "mean": round(float(np.mean(rolling_rho)), 4),
                    "trend": "increasing" if rolling_rho[-1] > rolling_rho[0] else "decreasing",
                }

        # --- Descriptive stats ---
        results["inflation_stats"] = {
            "mean": round(float(np.mean(pi)), 2),
            "std": round(float(np.std(pi, ddof=1)), 2),
            "latest": round(float(pi[-1]), 2),
            "max": round(float(np.max(pi)), 2),
            "min": round(float(np.min(pi)), 2),
        }

        # --- Score ---
        # AR(1) rho > 0.7 = unanchored = high stress
        # Linear mapping: rho in [-1, 1] -> score in [0, 100]
        # Score = clip(rho * 70 + 20, 0, 100) -- penalizes persistence
        rho_clipped = float(np.clip(rho, -1.0, 1.0))
        base_score = rho_clipped * 70.0 + 20.0

        # Penalty if current rolling rho is increasing (deteriorating anchoring)
        rolling_penalty = 0.0
        if "rolling_persistence" in results:
            rp = results["rolling_persistence"]
            if rp["trend"] == "increasing" and rp["current"] > 0.5:
                rolling_penalty = 10.0

        # Penalty for very high inflation volatility (hard to anchor)
        vol_penalty = min(float(results["inflation_stats"]["std"]) * 0.5, 10.0)

        score = float(np.clip(base_score + rolling_penalty + vol_penalty, 0.0, 100.0))

        return {"score": round(score, 1), "results": results}
