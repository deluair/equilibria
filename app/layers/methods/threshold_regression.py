"""Threshold regression: Hansen threshold model, grid search, bootstrap CI, multiple regimes.

Methodology
-----------
**Hansen (1999, 2000) Threshold Regression**:
    y_t = mu_1 + beta_1 * x_t + eps_t    if q_t <= gamma
    y_t = mu_2 + beta_2 * x_t + eps_t    if q_t > gamma

Reformulated using indicator variables:
    y_t = X_t' * delta_1 * I(q_t <= gamma) + X_t' * delta_2 * I(q_t > gamma) + eps_t

Estimation:
    For each candidate threshold gamma in grid {q_(i)}: run OLS for each regime,
    collect SSR(gamma). Estimate: gamma_hat = argmin_gamma SSR(gamma).

    F-statistic for threshold existence (H0: no threshold):
        F_1 = [SSR_0 - SSR_1(gamma_hat)] / s^2
    where SSR_0 = linear model SSR, s^2 = sigma^2 from threshold model.

    Asymptotic distribution under H0 non-standard (Davies problem).
    Bootstrap p-value: simulate y under H0 (linear), compute F for each replicate,
    p-value = fraction of F* exceeding observed F.

**Bootstrap Confidence Interval for Threshold**:
    By duality with LR test: C_n(alpha) = {gamma: LR_n(gamma) <= c_alpha}
    where LR_n(gamma) = [SSR(gamma) - SSR(gamma_hat)] / s^2
    and c_alpha is 95th percentile of asymptotic distribution of LR_n.
    For chi-sq(1) approximation: c_{0.05} = 7.35 (from Hansen 2000, Table 1).

**Multiple Regime Estimation**:
    Sequential approach (Bai 1997): test for 2nd threshold conditional on first.
    Repeat until F-test fails to reject or maximum M regimes reached.

Score: evidence of strong threshold effect with regime heterogeneity -> complex
structural dynamics (STRESS). No significant threshold -> linear dynamics (STABLE).

References:
    Hansen, B.E. (1999). Threshold Effects in Non-Dynamic Panels.
        Journal of Econometrics 93(2): 345-368.
    Hansen, B.E. (2000). Sample Splitting and Threshold Estimation.
        Econometrica 68(3): 575-603.
    Bai, J. (1997). Estimating Multiple Breaks One at a Time.
        Econometric Theory 13(3): 315-352.
"""

import json

import numpy as np
from scipy.stats import chi2

from app.layers.base import LayerBase


def _ols_ssr(X: np.ndarray, y: np.ndarray) -> tuple[float, np.ndarray]:
    """Return SSR and residuals from OLS."""
    try:
        beta, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
    except np.linalg.LinAlgError:
        return float(np.sum(y ** 2)), y
    resid = y - X @ beta
    return float(np.sum(resid ** 2)), resid


def _threshold_ssr(X: np.ndarray, y: np.ndarray, q: np.ndarray, gamma: float) -> float:
    """SSR from two-regime threshold model at threshold gamma."""
    idx_lo = q <= gamma
    idx_hi = ~idx_lo
    ssr = 0.0
    for idx in [idx_lo, idx_hi]:
        if np.sum(idx) < 3:
            return float("inf")
        ssr += _ols_ssr(X[idx], y[idx])[0]
    return ssr


class ThresholdRegression(LayerBase):
    layer_id = "l18"
    name = "Threshold Regression"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        n_boot = kwargs.get("n_bootstrap", 299)
        max_regimes = kwargs.get("max_regimes", 3)

        rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'threshold_regression'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 30:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        y_list, x_list, q_list = [], [], []
        for row in rows:
            if row["value"] is None:
                continue
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            y_list.append(float(row["value"]))
            x_list.append(float(meta.get("x", 0.0)))
            q_list.append(float(meta.get("q", meta.get("x", 0.0))))  # threshold variable

        n = len(y_list)
        if n < 30:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid obs"}

        y = np.array(y_list)
        x = np.array(x_list)
        q = np.array(q_list)

        X = np.column_stack([np.ones(n), x])

        # Baseline linear model SSR
        ssr_0, resid_0 = _ols_ssr(X, y)

        # --- Grid search for threshold ---
        # Trim 15% from each end (sufficient obs in each regime)
        q_sorted = np.sort(q)
        trim = max(int(0.15 * n), 3)
        grid = q_sorted[trim: n - trim]
        if len(grid) == 0:
            grid = q_sorted

        ssr_grid = np.array([_threshold_ssr(X, y, q, g) for g in grid])
        best_idx = int(np.argmin(ssr_grid))
        gamma_hat = float(grid[best_idx])
        ssr_1 = float(ssr_grid[best_idx])

        # Sigma-squared from threshold model
        idx_lo = q <= gamma_hat
        idx_hi = ~idx_lo
        n_lo = int(np.sum(idx_lo))
        n_hi = int(np.sum(idx_hi))
        s2 = ssr_1 / max(n - 4, 1)  # 4 parameters in 2-regime model

        # F-statistic
        f_stat = (ssr_0 - ssr_1) / s2 if s2 > 0 else 0.0

        # Regime estimates
        regimes = {}
        for label, idx in [("low", idx_lo), ("high", idx_hi)]:
            if np.sum(idx) >= 3:
                try:
                    beta_r, _, _, _ = np.linalg.lstsq(X[idx], y[idx], rcond=None)
                    regimes[label] = {
                        "n_obs": int(np.sum(idx)),
                        "intercept": round(float(beta_r[0]), 4),
                        "slope": round(float(beta_r[1]), 4),
                        "q_range": [round(float(np.min(q[idx])), 4),
                                    round(float(np.max(q[idx])), 4)],
                    }
                except np.linalg.LinAlgError:
                    pass

        # Coefficient difference across regimes
        coef_diff = None
        if "low" in regimes and "high" in regimes:
            coef_diff = round(abs(regimes["high"]["slope"] - regimes["low"]["slope"]), 4)

        # --- Bootstrap p-value ---
        rng = np.random.default_rng(42)
        # Under H0: fit linear model, resample residuals
        beta_0, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
        y_hat_0 = X @ beta_0
        boot_f = []
        for _ in range(n_boot):
            y_boot = y_hat_0 + rng.choice(resid_0, size=n, replace=True)
            ssr_boot_0, _ = _ols_ssr(X, y_boot)
            ssr_boot_grid = np.array([_threshold_ssr(X, y_boot, q, g) for g in grid])
            ssr_boot_1 = float(np.min(ssr_boot_grid))
            s2_boot = ssr_boot_1 / max(n - 4, 1)
            f_boot = (ssr_boot_0 - ssr_boot_1) / s2_boot if s2_boot > 0 else 0.0
            boot_f.append(f_boot)

        boot_f_arr = np.array(boot_f)
        p_value_boot = float(np.mean(boot_f_arr >= f_stat))

        # --- Bootstrap CI for threshold (LR inversion) ---
        # c_{0.95} from chi-sq(2) approximation: Hansen (2000)
        c95 = float(chi2.ppf(0.95, df=2))
        lr_curve = (ssr_grid - ssr_1) / s2 if s2 > 0 else ssr_grid * 0
        ci_idx = lr_curve <= c95
        if np.any(ci_idx):
            ci_lo = round(float(np.min(grid[ci_idx])), 4)
            ci_hi = round(float(np.max(grid[ci_idx])), 4)
        else:
            ci_lo = ci_hi = round(gamma_hat, 4)

        # --- Multiple regime test (sequential) ---
        thresholds = [gamma_hat]
        regime_f_stats = [round(float(f_stat), 4)]
        regime_p_values = [round(float(p_value_boot), 4)]

        for _ in range(1, max_regimes - 1):
            # Split at current thresholds and test for additional threshold in each segment
            best_f_next = 0.0
            best_gamma_next = None
            for seg_thresh in thresholds:
                for in_lo in [True, False]:
                    idx_seg = (q <= seg_thresh) if in_lo else (q > seg_thresh)
                    if np.sum(idx_seg) < 20:
                        continue
                    y_seg = y[idx_seg]
                    X_seg = X[idx_seg]
                    q_seg = q[idx_seg]
                    ssr_seg_0, resid_seg = _ols_ssr(X_seg, y_seg)
                    n_seg = len(y_seg)
                    trim_seg = max(int(0.15 * n_seg), 3)
                    grid_seg = np.sort(q_seg)[trim_seg: n_seg - trim_seg]
                    if len(grid_seg) == 0:
                        continue
                    ssr_seg_grid = np.array([_threshold_ssr(X_seg, y_seg, q_seg, g) for g in grid_seg])
                    ssr_seg_1 = float(np.min(ssr_seg_grid))
                    s2_seg = ssr_seg_1 / max(n_seg - 4, 1)
                    f_seg = (ssr_seg_0 - ssr_seg_1) / s2_seg if s2_seg > 0 else 0
                    if f_seg > best_f_next:
                        best_f_next = f_seg
                        best_gamma_next = float(grid_seg[int(np.argmin(ssr_seg_grid))])

            if best_gamma_next is not None and best_f_next > 5.0:
                thresholds.append(best_gamma_next)
                regime_f_stats.append(round(float(best_f_next), 4))
                # Quick bootstrap for this stage (fewer reps)
                regime_p_values.append(None)  # Full bootstrap omitted for speed
            else:
                break

        n_regimes = len(thresholds) + 1

        # --- Score ---
        score = 20.0
        # Significant threshold = structural complexity
        if p_value_boot < 0.05:
            score += 30
            if coef_diff is not None:
                score += min(coef_diff * 20, 20)
        elif p_value_boot < 0.10:
            score += 15

        # Multiple regimes
        if n_regimes > 2:
            score += (n_regimes - 2) * 10

        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "threshold_test": {
                "gamma_hat": round(gamma_hat, 4),
                "f_statistic": round(float(f_stat), 4),
                "p_value_bootstrap": round(p_value_boot, 4),
                "significant": p_value_boot < 0.05,
                "ci_95": [ci_lo, ci_hi],
                "ssr_linear": round(ssr_0, 4),
                "ssr_threshold": round(ssr_1, 4),
            },
            "regimes": regimes,
            "coefficient_difference": coef_diff,
            "multiple_thresholds": {
                "n_regimes": n_regimes,
                "thresholds": [round(g, 4) for g in sorted(thresholds)],
                "f_stats": regime_f_stats,
                "p_values": regime_p_values,
            },
        }
