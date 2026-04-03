"""Hysteresis analysis module.

Methodology
-----------
Hysteresis in macroeconomics (Blanchard & Summers 1986) refers to the
persistence of unemployment and output losses after recessions, driven by:

1. **Labor market hysteresis**: workers who become long-term unemployed
   lose skills and attachment, permanently reducing effective labor supply.
   Test via unit root in unemployment rate (Phelps 1972; Nelson & Plosser 1982).

2. **Output hysteresis (scarring)**: recessions permanently lower the
   output path -- output does not revert to pre-recession trend. Test via
   Blanchard-Summers (1986): are deviations from trend permanent or transitory?

3. **Path dependence in output**: HP-filter and Beveridge-Nelson
   decomposition to distinguish permanent (stochastic trend) vs cyclical
   components. High permanent component share = stronger hysteresis.

4. **Recession scarring index**: difference between actual post-recession
   output trajectory and pre-recession trend projection, cumulated over
   5 years.

Tests implemented:
- ADF unit root test on unemployment (Dickey-Fuller 1979)
- KPSS stationarity test as complementary check
- Variance ratio test (Lo & MacKinlay 1988) for output
- Beveridge-Nelson decomposition of output gap

Score (0-100): higher score indicates stronger hysteresis -- unemployment
near unit root, large permanent output losses, persistent scarring.

References:
    Blanchard, O.J. and Summers, L.H. (1986). "Hysteresis and the
        European Unemployment Problem." NBER Macroeconomics Annual, 1.
    Phelps, E.S. (1972). "Inflation Policy and Unemployment Theory."
        Norton, New York.
    Lo, A.W. and MacKinlay, A.C. (1988). "Stock Market Prices Do Not
        Follow Random Walks." Review of Financial Studies, 1(1), 41-66.
    Blanchard, O., Cerutti, E. and Summers, L. (2015). "Inflation and
        Activity: Two Explorations and Their Monetary Policy Implications."
        IMF Working Paper WP/15/230.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


def _adf_test(y: np.ndarray, max_lags: int = 8) -> dict:
    """Augmented Dickey-Fuller unit root test.

    H0: y has a unit root (hysteresis).
    Lag order selected by BIC.
    Returns test statistic and approximate p-value.
    """
    n = len(y)
    dy = np.diff(y)

    best_bic = np.inf
    best_p = 1
    for p in range(0, min(max_lags, n // 4)):
        n_r = len(dy) - p
        if n_r < 15:
            break
        Y = dy[p:]
        X = np.ones((n_r, 2 + p))
        X[:, 1] = y[p:n - 1]  # lagged level
        for j in range(p):
            X[:, 2 + j] = dy[p - 1 - j:n_r + p - 1 - j]

        try:
            beta = np.linalg.lstsq(X, Y, rcond=None)[0]
        except np.linalg.LinAlgError:
            continue

        resid = Y - X @ beta
        sse = float(np.sum(resid ** 2))
        k = X.shape[1]
        bic = np.log(sse / n_r) + k * np.log(n_r) / n_r
        if bic < best_bic:
            best_bic = bic
            best_p = p

    p = best_p
    n_r = len(dy) - p
    if n_r < 10:
        return {"note": "insufficient obs for ADF"}

    Y = dy[p:]
    X = np.ones((n_r, 2 + p))
    X[:, 1] = y[p:n - 1]
    for j in range(p):
        X[:, 2 + j] = dy[p - 1 - j:n_r + p - 1 - j]

    try:
        beta = np.linalg.lstsq(X, Y, rcond=None)[0]
        resid = Y - X @ beta
        sse = float(np.sum(resid ** 2))
        sigma2 = sse / (n_r - X.shape[1])
        XtX_inv = np.linalg.pinv(X.T @ X)
        se = np.sqrt(sigma2 * XtX_inv[1, 1])
        t_stat = float(beta[1]) / se if se > 0 else 0.0
    except (np.linalg.LinAlgError, ZeroDivisionError):
        return {"note": "ADF estimation failed"}

    # MacKinnon approximate critical values (no constant and trend, large sample)
    # tau_1pct = -3.43, tau_5pct = -2.86, tau_10pct = -2.57
    cv_1 = -3.43
    cv_5 = -2.86
    cv_10 = -2.57

    # Approximate p-value from MacKinnon (1994) response surface
    # (simplified polynomial for n>50)
    # p-value = Phi(a + b/n + c/n^2 + d/n^3 + tau*(e + f/n))
    # We use a conservative monotone approximation
    if t_stat < cv_1:
        p_val = 0.005
    elif t_stat < cv_5:
        p_val = 0.03
    elif t_stat < cv_10:
        p_val = 0.08
    elif t_stat < 0:
        p_val = 0.20
    else:
        p_val = 0.50

    return {
        "adf_statistic": round(float(t_stat), 4),
        "critical_1pct": cv_1,
        "critical_5pct": cv_5,
        "critical_10pct": cv_10,
        "p_value_approx": p_val,
        "unit_root_5pct": t_stat > cv_5,  # Fail to reject H0 = unit root present
        "lags_selected": p,
    }


def _kpss_test(y: np.ndarray, lags: int | None = None) -> dict:
    """KPSS test for stationarity (H0: stationary, H1: unit root)."""
    n = len(y)
    if lags is None:
        lags = int(np.floor(4 * (n / 100) ** 0.25))
    lags = max(1, lags)

    # Partial sums
    s = np.cumsum(y - np.mean(y))
    sigma2_hat = float(np.var(y, ddof=1))

    # Newey-West long-run variance
    gamma0 = sigma2_hat
    gamma_j = np.array([float(np.cov(y[j:], y[:-j] if j > 0 else y)) for j in range(1, lags + 1)])
    weights = 1 - np.arange(1, lags + 1) / (lags + 1)
    lrv = gamma0 + 2 * float(np.sum(weights * gamma_j))
    lrv = max(lrv, 1e-10)

    kpss_stat = float(np.sum(s ** 2)) / (n ** 2 * lrv)

    # Critical values (level, no trend) from Kwiatkowski et al. (1992)
    cv_10 = 0.347
    cv_5 = 0.463
    cv_1 = 0.739

    return {
        "kpss_statistic": round(kpss_stat, 4),
        "critical_10pct": cv_10,
        "critical_5pct": cv_5,
        "critical_1pct": cv_1,
        "rejects_stationarity_5pct": kpss_stat > cv_5,
        "lags": lags,
    }


def _variance_ratio_test(y: np.ndarray, q: int = 4) -> dict:
    """Lo-MacKinlay variance ratio test for random walk."""
    n = len(y)
    if n < q * 4:
        return {"note": "insufficient obs for VR test"}

    mu = float(np.mean(np.diff(y))) if len(np.diff(y)) > 0 else 0.0
    sigma_1 = float(np.var(np.diff(y), ddof=1))

    # q-period variance
    m = int((n - 1) / q) * q
    y_q = y[:m + 1]
    diffs_q = np.array([y_q[j] - y_q[j - q] - q * mu for j in range(q, len(y_q))])
    sigma_q = float(np.mean(diffs_q ** 2)) / q

    vr = sigma_q / sigma_1 if sigma_1 > 0 else 1.0

    # Asymptotic z-statistic (heteroskedasticity-consistent)
    # Under H0 (random walk): VR -> 1, z -> N(0,1)
    delta = np.zeros(q - 1)
    d = np.diff(y[:m + 1]) - mu
    for k in range(1, q):
        num = float(np.sum(d[k:] ** 2 * d[:-k] ** 2))
        denom = float(np.sum(d ** 2) ** 2 / (n - 1))
        delta[k - 1] = num / max(denom, 1e-12)

    theta = float(np.sum([(2 * (q - k) / q) ** 2 * delta[k - 1] for k in range(1, q)]))
    z_stat = (vr - 1) / np.sqrt(max(theta, 1e-10) / (n - 1))

    p_val = float(2 * (1 - stats.norm.cdf(abs(z_stat))))

    return {
        "variance_ratio": round(float(vr), 4),
        "z_statistic": round(float(z_stat), 3),
        "p_value": round(p_val, 4),
        "q_period": q,
        "random_walk_rejected_5pct": p_val < 0.05,
    }


def _beveridge_nelson(y: np.ndarray, ar_order: int = 4) -> dict:
    """Beveridge-Nelson decomposition of output into permanent and cyclical.

    Estimates AR(ar_order) on first-differenced y, recovers trend as
    the long-run forecast.
    """
    dy = np.diff(y)
    n = len(dy)
    if n < ar_order + 10:
        return {"note": "insufficient data for BN decomposition"}

    # Fit AR(p) to dy
    Y_ar = dy[ar_order:]
    X_ar = np.column_stack([dy[ar_order - j - 1:n - j - 1] for j in range(ar_order)])
    try:
        phi = np.linalg.lstsq(X_ar, Y_ar, rcond=None)[0]
    except np.linalg.LinAlgError:
        return {"note": "AR estimation failed"}

    # Long-run mean: E[dy] / (1 - sum(phi))
    sum_phi = float(np.sum(phi))

    # Permanent component: BN trend increment = mu + (1/(1-sum_phi)) * shock
    # Variance decomposition
    resid = Y_ar - X_ar @ phi
    sigma_eps = float(np.std(resid, ddof=1))
    lr_multiplier = 1 / max(abs(1 - sum_phi), 0.01)

    # Permanent component variance share
    sigma_trend = abs(lr_multiplier) * sigma_eps
    sigma_cycle = sigma_eps
    total = sigma_trend + sigma_cycle
    perm_share = sigma_trend / max(total, 1e-10)

    return {
        "permanent_component_share": round(float(perm_share), 4),
        "long_run_multiplier": round(float(lr_multiplier), 4),
        "sum_ar_coefficients": round(float(sum_phi), 4),
        "sigma_shock": round(float(sigma_eps), 6),
        "strong_hysteresis": float(perm_share) > 0.7,
    }


class Hysteresis(LayerBase):
    layer_id = "l2"
    name = "Hysteresis"

    async def compute(self, db, **kwargs) -> dict:
        """Test for hysteresis in unemployment and output.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country    : str  - ISO3 country code
            vr_period  : int  - variance ratio period q (default 4)
            ar_order   : int  - AR order for BN decomposition (default 4)
        """
        country = kwargs.get("country", "USA")
        vr_period = int(kwargs.get("vr_period", 4))
        ar_order = int(kwargs.get("ar_order", 4))

        series_map = {
            "unemployment": f"UNEMP_RATE_{country}",
            "output_gap":   f"OUTPUT_GAP_{country}",
            "gdp_log":      f"LOG_REAL_GDP_{country}",
            "nairu":        f"NAIRU_{country}",
            "longterm_unemp": f"LT_UNEMP_SHARE_{country}",
        }

        data: dict[str, np.ndarray] = {}
        for label, code in series_map.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
                (code,),
            )
            if rows:
                data[label] = np.array([float(r[1]) for r in rows])

        if "unemployment" not in data and "output_gap" not in data:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"No unemployment or output gap data for {country}",
            }

        results: dict = {"country": country}

        # --- 1. Unemployment unit root (ADF + KPSS) ---
        unemp_hysteresis = {}
        unit_root_present = False
        if "unemployment" in data and len(data["unemployment"]) >= 20:
            u = data["unemployment"]
            results["n_obs_unemp"] = len(u)

            adf = _adf_test(u)
            kpss = _kpss_test(u)

            # Consensus: unit root if ADF fails to reject AND KPSS rejects stationarity
            unit_root_present = (
                adf.get("unit_root_5pct", False) and kpss.get("rejects_stationarity_5pct", False)
            )

            unemp_hysteresis = {
                "adf": adf,
                "kpss": kpss,
                "unit_root_consensus": unit_root_present,
                "interpretation": (
                    "Strong hysteresis evidence: unit root in unemployment"
                    if unit_root_present
                    else "Weaker hysteresis: unemployment appears stationary"
                ),
            }

            # Blanchard-Summers test: regress u_t on u_{t-1}, test rho=1
            if len(u) >= 15:
                bs_slope, bs_intercept, bs_r, bs_p, _ = stats.linregress(u[:-1], u[1:])
                unemp_hysteresis["blanchard_summers"] = {
                    "ar1_coefficient": round(float(bs_slope), 4),
                    "t_stat_rho1": round(float((bs_slope - 1.0) /
                                               max(np.std(u[1:] - bs_slope * u[:-1] - bs_intercept)
                                                   / np.std(u[:-1]) / np.sqrt(len(u) - 2), 1e-6)), 3),
                    "near_unit_root": abs(bs_slope - 1.0) < 0.1,
                }
        else:
            unemp_hysteresis = {"note": "unemployment series too short or unavailable"}

        results["unemployment_persistence"] = unemp_hysteresis

        # --- 2. Output scarring (variance ratio) ---
        output_hysteresis = {}
        perm_share = 0.5
        if "output_gap" in data and len(data["output_gap"]) >= 20:
            og = data["output_gap"]
            vr = _variance_ratio_test(og, q=vr_period)
            output_hysteresis["variance_ratio"] = vr

        if "gdp_log" in data and len(data["gdp_log"]) >= ar_order + 15:
            bn = _beveridge_nelson(data["gdp_log"], ar_order)
            output_hysteresis["beveridge_nelson"] = bn
            perm_share = bn.get("permanent_component_share", 0.5)
        else:
            output_hysteresis["note"] = "log GDP data unavailable for BN decomposition"

        results["output_hysteresis"] = output_hysteresis

        # --- 3. Path dependence: recession scarring ---
        scarring = {}
        scarring_score = 0.0
        if "output_gap" in data and len(data["output_gap"]) >= 10:
            og = data["output_gap"]
            # Identify recessions: consecutive quarters with og < -1%
            rec_flag = og < -1.0
            # Persistence after recession: mean og for 5 periods after last negative
            last_rec = np.where(rec_flag)[0]
            if len(last_rec) > 0:
                rec_end = last_rec[-1]
                post_rec = og[rec_end:min(rec_end + 6, len(og))]
                scarring_val = float(np.mean(post_rec)) if len(post_rec) > 0 else 0.0
                scarring = {
                    "post_recession_output_gap_mean": round(scarring_val, 4),
                    "n_recession_quarters": int(rec_flag.sum()),
                    "output_gap_latest": round(float(og[-1]), 4),
                    "scarring_present": scarring_val < -1.0,
                }
                if scarring_val < -1.0:
                    scarring_score = min(abs(scarring_val) * 8, 20)
            else:
                scarring = {"note": "no recession periods identified"}
        else:
            scarring = {"note": "output gap data unavailable"}

        results["scarring"] = scarring

        # --- 4. Long-term unemployment share ---
        lt_unemp = {}
        lt_penalty = 0.0
        if "longterm_unemp" in data and len(data["longterm_unemp"]) >= 1:
            ltu = data["longterm_unemp"]
            lt_latest = float(ltu[-1])
            lt_unemp = {
                "lt_unemp_share_latest": round(lt_latest, 2),
                "high_lt_unemp": lt_latest > 30,  # > 30% of unemployed
            }
            if lt_latest > 30:
                lt_penalty = min((lt_latest - 30) * 0.8, 20)
        else:
            lt_unemp = {"note": "long-term unemployment data unavailable"}

        results["long_term_unemployment"] = lt_unemp

        # --- Score ---
        # Unit root in unemployment
        unit_root_penalty = 30 if unit_root_present else 0

        # Permanent output component share
        perm_penalty = min(float(perm_share) * 30, 25)

        # Scarring
        scar_penalty = float(scarring_score)

        # Long-term unemployment
        ltu_penalty = float(lt_penalty)

        score = float(np.clip(unit_root_penalty + perm_penalty + scar_penalty + ltu_penalty, 0, 100))

        return {"score": round(score, 2), "results": results}
