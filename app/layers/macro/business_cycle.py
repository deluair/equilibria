"""Business Cycle analysis module.

Methodology
-----------
Three filtering/decomposition approaches for trend-cycle separation:

1. **Hodrick-Prescott (HP) filter** (Hodrick & Prescott 1997):
   Minimizes sum of squared deviations from trend subject to a smoothness
   penalty. Lambda = 1600 for quarterly data (Ravn-Uhlig rule: 1600 * (freq/4)^4).
   Known issues: endpoint instability, spurious cycles (Hamilton 2018 critique).

2. **Hamilton (2018) filter**:
   Regress y_{t+h} on y_t, y_{t-1}, ..., y_{t-p+1}.
   Residuals are the cyclical component. Default: h=8, p=4 for quarterly data.
   Avoids HP filter's spurious dynamics and endpoint problems.

3. **Beveridge-Nelson (BN) decomposition** (Beveridge & Nelson 1981):
   Decomposes a series into permanent (random walk + drift) and transitory
   (stationary) components using an ARIMA model. The permanent component
   is the long-run forecast minus the deterministic trend.

Cycle dating:
- Peak: local maximum in smoothed cyclical component
- Trough: local minimum in smoothed cyclical component
- Duration and amplitude statistics

Output gap estimation from all three methods with cross-comparison.

Score reflects cycle position and consistency across methods.

Sources: FRED (real GDP, industrial production)
"""

import numpy as np
from scipy import sparse
from scipy.sparse.linalg import spsolve
from scipy.signal import argrelextrema

from app.layers.base import LayerBase


def _hp_filter(y: np.ndarray, lamb: float = 1600.0) -> tuple[np.ndarray, np.ndarray]:
    """Hodrick-Prescott filter. Returns (trend, cycle)."""
    n = len(y)
    # Second-difference matrix
    e = np.eye(n)
    d = np.diff(e, n=2, axis=0)
    # (I + lambda * D'D) * tau = y
    trend = np.linalg.solve(np.eye(n) + lamb * d.T @ d, y)
    cycle = y - trend
    return trend, cycle


def _hamilton_filter(y: np.ndarray, h: int = 8, p: int = 4) -> tuple[np.ndarray, np.ndarray, dict]:
    """Hamilton (2018) filter. Returns (trend, cycle, regression_info)."""
    n = len(y)
    if n < h + p + 1:
        raise ValueError(f"Need at least {h + p + 1} observations, got {n}")

    # Build regression: y_{t+h} on y_t, y_{t-1}, ..., y_{t-p+1}
    # Effective sample: t from p-1 to n-h-1
    Y = y[h + p - 1:]
    X_cols = [np.ones(len(Y))]
    for j in range(p):
        X_cols.append(y[p - 1 - j : n - h - j])
    X = np.column_stack(X_cols)

    beta = np.linalg.lstsq(X, Y, rcond=None)[0]
    fitted = X @ beta
    resid = Y - fitted

    # Align to original index
    trend = np.full(n, np.nan)
    cycle = np.full(n, np.nan)
    trend[h + p - 1:] = fitted
    cycle[h + p - 1:] = resid

    r_squared = 1 - np.sum(resid ** 2) / np.sum((Y - np.mean(Y)) ** 2)

    return trend, cycle, {"r_squared": float(r_squared), "beta": beta.tolist(), "h": h, "p": p}


def _bn_decomposition(y: np.ndarray, max_ar: int = 4) -> tuple[np.ndarray, np.ndarray, dict]:
    """Beveridge-Nelson decomposition using AR(p) representation of first differences.

    Selects lag order by BIC.
    """
    dy = np.diff(y)
    n = len(dy)

    # Select AR order by BIC
    best_bic = np.inf
    best_p = 1

    for p in range(1, min(max_ar + 1, n // 3)):
        Y_ar = dy[p:]
        X_cols = [np.ones(len(Y_ar))]
        for j in range(1, p + 1):
            X_cols.append(dy[p - j : n - j])
        X_ar = np.column_stack(X_cols)

        beta_ar = np.linalg.lstsq(X_ar, Y_ar, rcond=None)[0]
        resid_ar = Y_ar - X_ar @ beta_ar
        sigma2 = float(np.sum(resid_ar ** 2)) / len(Y_ar)
        bic = len(Y_ar) * np.log(sigma2 + 1e-12) + (p + 1) * np.log(len(Y_ar))
        if bic < best_bic:
            best_bic = bic
            best_p = p

    # Fit chosen AR(p) to first differences
    p = best_p
    Y_ar = dy[p:]
    X_cols = [np.ones(len(Y_ar))]
    for j in range(1, p + 1):
        X_cols.append(dy[p - j : n - j])
    X_ar = np.column_stack(X_cols)
    beta_ar = np.linalg.lstsq(X_ar, Y_ar, rcond=None)[0]

    # BN permanent component: sum of long-run MA coefficients
    # For AR(p): long-run multiplier = 1 / (1 - sum(phi_i))
    ar_coeffs = beta_ar[1:]  # exclude intercept
    phi_sum = float(np.sum(ar_coeffs))
    long_run_mult = 1.0 / (1.0 - phi_sum) if abs(1.0 - phi_sum) > 0.01 else 1.0
    drift = float(beta_ar[0]) * long_run_mult

    # BN transitory component: computed from cumulated forecast revisions
    # Approximate: cycle_t = -(sum_{j=1}^{inf} E_t[dy_{t+j}] - drift)
    # Using AR representation truncated at 100 steps
    resid_ar = Y_ar - X_ar @ beta_ar

    # Build cycle via cumulated conditional expectations
    cycle = np.zeros(len(y))
    for t in range(p, n):
        # Forecast from current AR state
        state = dy[t - p + 1 : t + 1][::-1]  # most recent first
        cum_forecast = 0.0
        current_state = state.copy()
        for step in range(1, 101):
            pred = float(beta_ar[0]) + float(np.dot(ar_coeffs, current_state[:p]))
            cum_forecast += pred - drift
            # Shift state
            current_state = np.concatenate([[pred], current_state[:-1]])
        cycle[t + 1] = -cum_forecast if t + 1 < len(y) else 0

    trend = y - cycle

    return trend, cycle, {"ar_order": p, "bic": float(best_bic), "drift": drift}


class BusinessCycle(LayerBase):
    layer_id = "l2"
    name = "Business Cycle"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        hp_lambda = kwargs.get("hp_lambda", 1600.0)

        # Fetch real GDP (log level)
        gdp_rows = await db.execute_fetchall(
            "SELECT date, value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
            (f"GDP_{country}",),
        )

        if not gdp_rows or len(gdp_rows) < 20:
            return {"score": 50, "results": {"error": "insufficient GDP data"}}

        dates = [r[0] for r in gdp_rows]
        y = np.array([float(r[1]) for r in gdp_rows])
        log_y = np.log(y)

        results = {
            "country": country,
            "n_obs": len(y),
            "period": f"{dates[0]} to {dates[-1]}",
        }

        # --- 1. HP Filter ---
        hp_trend, hp_cycle = _hp_filter(log_y, lamb=hp_lambda)
        hp_gap = hp_cycle * 100  # percent

        results["hp_filter"] = {
            "lambda": hp_lambda,
            "output_gap_latest": float(hp_gap[-1]),
            "output_gap_mean": float(np.mean(hp_gap)),
            "output_gap_std": float(np.std(hp_gap, ddof=1)),
            "gap_series": hp_gap.tolist(),
            "dates": dates,
        }

        # --- 2. Hamilton Filter ---
        h = kwargs.get("hamilton_h", 8)
        p = kwargs.get("hamilton_p", 4)
        try:
            ham_trend, ham_cycle, ham_info = _hamilton_filter(log_y, h=h, p=p)
            # Convert to percent gap where available
            valid = ~np.isnan(ham_cycle)
            ham_gap = ham_cycle * 100

            results["hamilton_filter"] = {
                "h": h,
                "p": p,
                "r_squared": ham_info["r_squared"],
                "output_gap_latest": float(ham_gap[-1]) if valid[-1] else None,
                "gap_series": [float(v) if not np.isnan(v) else None for v in ham_gap],
                "dates": dates,
            }
        except ValueError as exc:
            results["hamilton_filter"] = {"error": str(exc)}

        # --- 3. Beveridge-Nelson Decomposition ---
        try:
            bn_trend, bn_cycle, bn_info = _bn_decomposition(log_y)
            bn_gap = bn_cycle * 100

            results["beveridge_nelson"] = {
                "ar_order": bn_info["ar_order"],
                "bic": bn_info["bic"],
                "drift_annualized": bn_info["drift"] * 4 * 100,  # quarterly to annual %
                "output_gap_latest": float(bn_gap[-1]),
                "gap_series": bn_gap.tolist(),
                "dates": dates,
            }
        except Exception as exc:
            results["beveridge_nelson"] = {"error": str(exc)}

        # --- Cycle dating ---
        # Use HP cycle (most common for dating)
        smooth_cycle = np.convolve(hp_cycle, np.ones(3) / 3, mode="same")
        # Find peaks and troughs
        peak_idx = argrelextrema(smooth_cycle, np.greater, order=4)[0]
        trough_idx = argrelextrema(smooth_cycle, np.less, order=4)[0]

        peaks = [{"date": dates[i], "gap_pct": float(hp_gap[i])} for i in peak_idx]
        troughs = [{"date": dates[i], "gap_pct": float(hp_gap[i])} for i in trough_idx]

        # Expansion/contraction durations
        durations = []
        all_turns = sorted(
            [(i, "peak") for i in peak_idx] + [(i, "trough") for i in trough_idx],
            key=lambda x: x[0],
        )
        for j in range(1, len(all_turns)):
            dur = all_turns[j][0] - all_turns[j - 1][0]
            phase = "expansion" if all_turns[j][1] == "peak" else "contraction"
            durations.append({
                "phase": phase,
                "start": dates[all_turns[j - 1][0]],
                "end": dates[all_turns[j][0]],
                "quarters": dur,
            })

        results["cycle_dating"] = {
            "peaks": peaks,
            "troughs": troughs,
            "durations": durations,
            "n_complete_cycles": min(len(peaks), len(troughs)),
        }

        # --- Cross-method comparison ---
        gaps_latest = {}
        if "hp_filter" in results:
            gaps_latest["hp"] = results["hp_filter"]["output_gap_latest"]
        if "hamilton_filter" in results and results["hamilton_filter"].get("output_gap_latest") is not None:
            gaps_latest["hamilton"] = results["hamilton_filter"]["output_gap_latest"]
        if "beveridge_nelson" in results and "output_gap_latest" in results["beveridge_nelson"]:
            gaps_latest["beveridge_nelson"] = results["beveridge_nelson"]["output_gap_latest"]

        if len(gaps_latest) > 1:
            vals = list(gaps_latest.values())
            results["cross_method"] = {
                "latest_gaps": gaps_latest,
                "mean": float(np.mean(vals)),
                "spread": float(np.max(vals) - np.min(vals)),
                "agreement": float(np.max(vals) - np.min(vals)) < 2.0,
            }

        # --- Score ---
        # Negative output gap -> recession risk -> stress
        latest_gap = float(hp_gap[-1])
        if latest_gap < -3:
            gap_penalty = 40
        elif latest_gap < -1:
            gap_penalty = 20
        elif latest_gap > 3:
            gap_penalty = 25  # overheating
        else:
            gap_penalty = 5

        # Method disagreement
        disagree_penalty = 0
        if "cross_method" in results and not results["cross_method"]["agreement"]:
            disagree_penalty = 15

        # Cycle volatility
        vol_penalty = min(float(np.std(hp_gap, ddof=1)) * 5, 25)

        score = min(gap_penalty + disagree_penalty + vol_penalty, 100)

        return {"score": round(score, 1), "results": results}
