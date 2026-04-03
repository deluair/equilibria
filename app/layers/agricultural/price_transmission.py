"""Commodity price transmission analysis using VECM.

Estimates the speed and symmetry of price transmission between commodity
markets (e.g., international to domestic, wholesale to retail) using
Vector Error Correction Models (VECM). Tests for threshold cointegration
and asymmetric adjustment.

Methodology:
    1. **Cointegration testing**: Engle-Granger two-step procedure.
       Step 1: Estimate long-run relationship p_d = a + b*p_w + u
       Step 2: Test residuals for stationarity (ADF test on residuals).

    2. **VECM estimation**:
       Dp_d,t = a1 + g1*ECT_{t-1} + sum(d1_i*Dp_d,{t-i}) + sum(d2_i*Dp_w,{t-i}) + e1_t
       Dp_w,t = a2 + g2*ECT_{t-1} + sum(d3_i*Dp_d,{t-i}) + sum(d4_i*Dp_w,{t-i}) + e2_t
       where ECT = p_d - a - b*p_w (error correction term)
       g1, g2 = speed of adjustment parameters

    3. **Asymmetric price transmission** (Houck 1977, von Cramon-Taubadel 1998):
       Split ECT into positive and negative components:
       ECT+ = max(ECT, 0), ECT- = min(ECT, 0)
       Test H0: g1+ = g1- (symmetric adjustment)

    4. **Threshold cointegration** (Enders & Siklos 2001):
       Allow regime-dependent adjustment with threshold tau:
       ECT_t = I_t * g1+ * ECT_{t-1} + (1-I_t) * g1- * ECT_{t-1}
       where I_t = 1 if ECT_{t-1} >= tau, 0 otherwise.

Score (0-100): Higher score indicates poor/asymmetric price transmission,
suggesting market distortions, market power, or trade barriers.

References:
    Engle, R.F., Granger, C.W.J. (1987). "Co-integration and error
        correction." Econometrica, 55(2), 251-276.
    von Cramon-Taubadel, S. (1998). "Estimating asymmetric price
        transmission with the error correction representation."
        European Review of Agricultural Economics, 25(1), 1-18.
    Enders, W., Siklos, P.L. (2001). "Cointegration and threshold
        adjustment." Journal of Business & Economic Statistics, 19(2).
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class PriceTransmission(LayerBase):
    layer_id = "l5"
    name = "Price Transmission"

    COMMODITIES = ("wheat", "rice", "maize", "soybeans", "sugar")

    async def compute(self, db, **kwargs) -> dict:
        """Estimate price transmission between international and domestic markets.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code
            commodities : tuple - commodities to analyze
            lags : int - number of lags in VECM (default 2)
        """
        country = kwargs.get("country_iso3", "BGD")
        commodities = kwargs.get("commodities", self.COMMODITIES)
        n_lags = kwargs.get("lags", 2)

        results = {}
        adjustment_speeds = []

        for commodity in commodities:
            # Fetch domestic price series
            domestic_rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.source IN ('national', 'fao', 'cpi')
                  AND ds.name LIKE ?
                ORDER BY dp.date ASC
                """,
                (country, f"%{commodity}%domestic%price%"),
            )

            # Fetch international price series
            world_rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.source IN ('wb_commodity', 'imf', 'fred')
                  AND ds.name LIKE ?
                ORDER BY dp.date ASC
                """,
                (f"%{commodity}%world%price%",),
            )

            if len(domestic_rows) < 15 or len(world_rows) < 15:
                results[commodity] = {"status": "insufficient_data"}
                continue

            # Align by date
            dom_by_date = {r["date"]: r["value"] for r in domestic_rows}
            world_by_date = {r["date"]: r["value"] for r in world_rows}
            common_dates = sorted(set(dom_by_date) & set(world_by_date))

            if len(common_dates) < 15:
                results[commodity] = {"status": "insufficient_overlap"}
                continue

            p_d = np.array([dom_by_date[d] for d in common_dates], dtype=float)
            p_w = np.array([world_by_date[d] for d in common_dates], dtype=float)

            # Filter positive prices
            valid = (p_d > 0) & (p_w > 0)
            p_d = p_d[valid]
            p_w = p_w[valid]

            if len(p_d) < 15:
                results[commodity] = {"status": "insufficient_valid_obs"}
                continue

            # Log transform
            ln_pd = np.log(p_d)
            ln_pw = np.log(p_w)

            # Step 1: Cointegration test (Engle-Granger)
            coint_result = self._engle_granger_test(ln_pd, ln_pw)

            # Step 2: VECM estimation
            vecm_result = self._estimate_vecm(ln_pd, ln_pw, n_lags)

            # Step 3: Asymmetric price transmission
            asymmetry_result = self._test_asymmetry(ln_pd, ln_pw, n_lags)

            # Step 4: Threshold cointegration
            threshold_result = self._threshold_cointegration(ln_pd, ln_pw, n_lags)

            results[commodity] = {
                "status": "ok",
                "n_obs": len(p_d),
                "cointegration": coint_result,
                "vecm": vecm_result,
                "asymmetry": asymmetry_result,
                "threshold": threshold_result,
            }

            if vecm_result.get("speed_of_adjustment") is not None:
                adjustment_speeds.append(abs(vecm_result["speed_of_adjustment"]))

        if not adjustment_speeds:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data for any commodity",
            }

        # Score: slow/asymmetric transmission -> high score
        mean_speed = float(np.mean(adjustment_speeds))
        # Speed of adjustment near 0 = very slow transmission
        # Speed near 1 = instant transmission
        # Normalize: score = (1 - mean_speed) * 100
        speed_component = max(0, min(70, (1.0 - min(mean_speed, 1.0)) * 70))

        # Asymmetry penalty
        n_asymmetric = sum(
            1 for r in results.values()
            if isinstance(r, dict) and r.get("asymmetry", {}).get("is_asymmetric", False)
        )
        asymmetry_component = min(30, n_asymmetric * 10)

        score = speed_component + asymmetry_component

        return {
            "score": round(max(0.0, min(100.0, score)), 2),
            "country": country,
            "n_commodities_analyzed": len([r for r in results.values() if isinstance(r, dict) and r.get("status") == "ok"]),
            "mean_adjustment_speed": round(mean_speed, 4),
            "commodity_results": results,
        }

    @staticmethod
    def _engle_granger_test(y: np.ndarray, x: np.ndarray) -> dict:
        """Engle-Granger two-step cointegration test.

        Step 1: OLS y = a + b*x + u
        Step 2: ADF test on residuals u_hat
        """
        n = len(y)
        X = np.column_stack([np.ones(n), x])
        beta = np.linalg.lstsq(X, y, rcond=None)[0]
        residuals = y - X @ beta

        # ADF test on residuals: Du_t = rho*u_{t-1} + e_t
        du = np.diff(residuals)
        u_lag = residuals[:-1]
        n_adf = len(du)
        X_adf = np.column_stack([u_lag])
        try:
            rho = np.linalg.lstsq(X_adf, du, rcond=None)[0][0]
        except np.linalg.LinAlgError:
            return {"cointegrated": False, "error": "ADF estimation failed"}

        resid_adf = du - X_adf @ np.array([rho])
        sigma2 = float(np.sum(resid_adf ** 2)) / max(n_adf - 1, 1)
        se_rho = float(np.sqrt(sigma2 / max(np.sum(u_lag ** 2), 1e-10)))
        adf_stat = rho / se_rho if se_rho > 0 else 0.0

        # Critical values for EG test (approximate, n=100)
        # 5% critical value ~ -3.37 (two variables)
        critical_5pct = -3.37
        cointegrated = adf_stat < critical_5pct

        return {
            "long_run_coeff": round(float(beta[1]), 4),
            "long_run_intercept": round(float(beta[0]), 4),
            "adf_statistic": round(float(adf_stat), 4),
            "critical_value_5pct": critical_5pct,
            "cointegrated": bool(cointegrated),
        }

    @staticmethod
    def _estimate_vecm(y: np.ndarray, x: np.ndarray, lags: int) -> dict:
        """Estimate Vector Error Correction Model.

        Dp_d,t = a + g*ECT_{t-1} + sum(d_i*Dp_d,{t-i}) + sum(d_i*Dp_w,{t-i}) + e
        """
        n = len(y)

        # Long-run relationship for ECT
        X_lr = np.column_stack([np.ones(n), x])
        beta_lr = np.linalg.lstsq(X_lr, y, rcond=None)[0]
        ect = y - X_lr @ beta_lr  # error correction term

        # Differences
        dy = np.diff(y)
        dx = np.diff(x)
        ect_lag = ect[:-1]

        # Effective sample after lags
        T = len(dy) - lags
        if T < 5:
            return {"status": "insufficient_obs_for_lags"}

        # Build VECM design matrix for domestic price equation
        dep = dy[lags:]
        regressors = [np.ones(T), ect_lag[lags:]]
        for lag in range(1, lags + 1):
            regressors.append(dy[lags - lag: -lag if lag > 0 else T + lags - lag])
            regressors.append(dx[lags - lag: -lag if lag > 0 else T + lags - lag])

        X_vecm = np.column_stack(regressors)

        try:
            beta_vecm = np.linalg.lstsq(X_vecm, dep, rcond=None)[0]
        except np.linalg.LinAlgError:
            return {"status": "estimation_failed"}

        gamma = beta_vecm[1]  # speed of adjustment

        resid = dep - X_vecm @ beta_vecm
        ss_res = float(np.sum(resid ** 2))
        ss_tot = float(np.sum((dep - np.mean(dep)) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        # Standard error of gamma
        k = X_vecm.shape[1]
        sigma2 = ss_res / max(T - k, 1)
        try:
            XtX_inv = np.linalg.inv(X_vecm.T @ X_vecm)
            se_gamma = float(np.sqrt(sigma2 * XtX_inv[1, 1]))
        except np.linalg.LinAlgError:
            se_gamma = None

        # Half-life of adjustment (periods for 50% of shock to dissipate)
        half_life = np.log(0.5) / np.log(1 + gamma) if -1 < gamma < 0 else None

        return {
            "speed_of_adjustment": round(float(gamma), 4),
            "se_adjustment": round(se_gamma, 4) if se_gamma is not None else None,
            "t_stat": round(float(gamma / se_gamma), 4) if se_gamma and se_gamma > 0 else None,
            "half_life_periods": round(float(half_life), 2) if half_life is not None else None,
            "r_squared": round(r2, 4),
            "n_obs": T,
            "lags": lags,
            "long_run_elasticity": round(float(beta_lr[1]), 4),
        }

    @staticmethod
    def _test_asymmetry(y: np.ndarray, x: np.ndarray, lags: int) -> dict:
        """Test for asymmetric price transmission.

        Split ECT into positive (ECT+) and negative (ECT-) components:
        Dy_t = a + g+*ECT+_{t-1} + g-*ECT-_{t-1} + lagged diffs + e
        Test H0: g+ = g- (symmetric)
        """
        n = len(y)
        X_lr = np.column_stack([np.ones(n), x])
        beta_lr = np.linalg.lstsq(X_lr, y, rcond=None)[0]
        ect = y - X_lr @ beta_lr

        dy = np.diff(y)
        dx = np.diff(x)
        ect_lag = ect[:-1]

        # Split ECT
        ect_pos = np.maximum(ect_lag, 0)
        ect_neg = np.minimum(ect_lag, 0)

        T = len(dy) - lags
        if T < 8:
            return {"status": "insufficient_obs"}

        dep = dy[lags:]
        regressors = [np.ones(T), ect_pos[lags:], ect_neg[lags:]]
        for lag in range(1, lags + 1):
            regressors.append(dy[lags - lag: -lag if lag > 0 else T + lags - lag])
            regressors.append(dx[lags - lag: -lag if lag > 0 else T + lags - lag])

        X_asym = np.column_stack(regressors)

        try:
            beta_asym = np.linalg.lstsq(X_asym, dep, rcond=None)[0]
        except np.linalg.LinAlgError:
            return {"status": "estimation_failed"}

        gamma_pos = beta_asym[1]
        gamma_neg = beta_asym[2]

        resid = dep - X_asym @ beta_asym
        ss_res = float(np.sum(resid ** 2))
        k = X_asym.shape[1]
        ss_res / max(T - k, 1)

        # F-test for H0: gamma_pos = gamma_neg
        # Restricted model: single gamma
        regressors_r = [np.ones(T), ect_lag[lags:]]
        for lag in range(1, lags + 1):
            regressors_r.append(dy[lags - lag: -lag if lag > 0 else T + lags - lag])
            regressors_r.append(dx[lags - lag: -lag if lag > 0 else T + lags - lag])
        X_sym = np.column_stack(regressors_r)

        try:
            beta_sym = np.linalg.lstsq(X_sym, dep, rcond=None)[0]
            resid_sym = dep - X_sym @ beta_sym
            ss_res_r = float(np.sum(resid_sym ** 2))
        except np.linalg.LinAlgError:
            return {"status": "restricted_estimation_failed"}

        # F-statistic: ((SSR_r - SSR_u) / q) / (SSR_u / (T - k))
        q = 1  # one restriction
        f_stat = ((ss_res_r - ss_res) / q) / (ss_res / max(T - k, 1)) if ss_res > 0 else 0.0
        p_value = 1.0 - stats.f.cdf(f_stat, q, max(T - k, 1))

        return {
            "gamma_positive": round(float(gamma_pos), 4),
            "gamma_negative": round(float(gamma_neg), 4),
            "f_statistic": round(float(f_stat), 4),
            "p_value": round(float(p_value), 4),
            "is_asymmetric": bool(p_value < 0.05),
            "faster_adjustment": "positive_deviations" if abs(gamma_pos) > abs(gamma_neg) else "negative_deviations",
        }

    @staticmethod
    def _threshold_cointegration(y: np.ndarray, x: np.ndarray, lags: int) -> dict:
        """Threshold cointegration (Enders & Siklos 2001).

        Search for optimal threshold tau that minimizes residual sum of squares.
        """
        n = len(y)
        X_lr = np.column_stack([np.ones(n), x])
        beta_lr = np.linalg.lstsq(X_lr, y, rcond=None)[0]
        ect = y - X_lr @ beta_lr

        dy = np.diff(y)
        ect_lag = ect[:-1]

        T = len(dy) - lags
        if T < 10:
            return {"status": "insufficient_obs"}

        dep = dy[lags:]
        ect_l = ect_lag[lags:]

        # Grid search over threshold values (15th to 85th percentile of ECT)
        tau_candidates = np.percentile(ect_l, np.arange(15, 86, 5))
        best_tau = 0.0
        best_ssr = np.inf
        best_gammas = (0.0, 0.0)

        for tau in tau_candidates:
            indicator = (ect_l >= tau).astype(float)
            ect_above = indicator * ect_l
            ect_below = (1 - indicator) * ect_l

            X_thresh = np.column_stack([np.ones(T), ect_above, ect_below])
            try:
                beta_t = np.linalg.lstsq(X_thresh, dep, rcond=None)[0]
                resid_t = dep - X_thresh @ beta_t
                ssr = float(np.sum(resid_t ** 2))
                if ssr < best_ssr:
                    best_ssr = ssr
                    best_tau = tau
                    best_gammas = (beta_t[1], beta_t[2])
            except np.linalg.LinAlgError:
                continue

        if best_ssr == np.inf:
            return {"status": "estimation_failed"}

        return {
            "optimal_threshold": round(float(best_tau), 4),
            "gamma_above": round(float(best_gammas[0]), 4),
            "gamma_below": round(float(best_gammas[1]), 4),
            "regime_dependent": abs(best_gammas[0] - best_gammas[1]) > 0.05,
            "ssr": round(best_ssr, 4),
        }
