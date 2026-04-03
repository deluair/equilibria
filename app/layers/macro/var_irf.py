"""VAR Impulse Response - VAR estimation, Cholesky and structural identification, IRF, FEVD."""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class VARImpulseResponse(LayerBase):
    layer_id = "l2"
    name = "VAR Impulse Response"
    weight = 0.05

    # Default VAR specification (Christiano-Eichenbaum-Evans ordering)
    DEFAULT_VARIABLES = {
        "gdp": "GDP",
        "cpi": "CPIAUCSL",
        "fed_funds": "FEDFUNDS",
        "m2": "M2SL",
        "sp500": "SP500",
    }

    # Standard macro VAR orderings
    ORDERINGS = {
        "monetary_policy": ["gdp", "cpi", "fed_funds", "m2"],
        "financial": ["gdp", "cpi", "fed_funds", "sp500"],
        "full": ["gdp", "cpi", "fed_funds", "m2", "sp500"],
    }

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 25)
        var_lags = kwargs.get("lags", 4)
        ordering = kwargs.get("ordering", "monetary_policy")
        irf_horizon = kwargs.get("irf_horizon", 24)
        n_bootstrap = kwargs.get("n_bootstrap", 500)

        var_names = self.ORDERINGS.get(ordering, self.ORDERINGS["monetary_policy"])
        series_ids = [self.DEFAULT_VARIABLES[name] for name in var_names if name in self.DEFAULT_VARIABLES]

        rows = await db.execute_fetchall(
            """
            SELECT series_id, date, value FROM data_points
            WHERE series_id IN ({})
              AND country_code = ?
              AND date >= date('now', ?)
            ORDER BY series_id, date
            """.format(",".join("?" for _ in series_ids)),
            (*series_ids, country, f"-{lookback} years"),
        )

        series_map: dict[str, dict[str, float]] = {}
        for r in rows:
            series_map.setdefault(r["series_id"], {})[r["date"]] = float(r["value"])

        # Align to common dates
        all_dates = set()
        for data in series_map.values():
            all_dates |= set(data.keys())
        sorted(all_dates)

        # Build data matrix
        available_names = []
        available_ids = []
        for name in var_names:
            sid = self.DEFAULT_VARIABLES.get(name)
            if sid and sid in series_map:
                available_names.append(name)
                available_ids.append(sid)

        if len(available_names) < 2:
            return {"score": 50.0, "results": {}, "note": "insufficient variables"}

        # Common dates with all variables present
        full_dates = sorted(set.intersection(*[set(series_map[sid].keys()) for sid in available_ids]))
        if len(full_dates) < var_lags + 24:
            return {"score": 50.0, "results": {}, "note": "insufficient aligned observations"}

        # Build endogenous matrix with appropriate transformations
        raw_matrix = np.column_stack([
            [series_map[sid][d] for d in full_dates]
            for sid in available_ids
        ])

        # Transform: log-diff for GDP/CPI/M2/SP500, level for fed_funds
        Y, transform_info = self._transform_data(raw_matrix, available_names)
        if Y is None or Y.shape[0] < var_lags + 12:
            return {"score": 50.0, "results": {}, "note": "insufficient data after transformation"}

        results = {}

        # Estimate VAR
        var_est = self._estimate_var(Y, var_lags)
        if var_est is None:
            return {"score": 50.0, "results": {}, "note": "VAR estimation failed"}

        B, Sigma, residuals, n_obs = var_est
        k = Y.shape[1]

        results["var_info"] = {
            "variables": available_names,
            "ordering": ordering,
            "lags": var_lags,
            "n_obs": n_obs,
            "transforms": transform_info,
        }

        # Information criteria for lag selection
        aic, bic = self._information_criteria(residuals, k, var_lags, n_obs)
        results["lag_selection"] = {"aic": aic, "bic": bic}

        # Optimal lag (test 1-8)
        optimal_lags = self._select_lags(Y, max_lags=8)
        results["lag_selection"]["optimal_aic"] = optimal_lags["aic"]
        results["lag_selection"]["optimal_bic"] = optimal_lags["bic"]

        # Cholesky IRF
        P = np.linalg.cholesky(Sigma)
        irfs = self._compute_irf(B, k, var_lags, P, irf_horizon)

        # Format IRFs
        irf_formatted = {}
        for shock_idx, shock_name in enumerate(available_names):
            irf_formatted[shock_name] = {}
            for resp_idx, resp_name in enumerate(available_names):
                irf_formatted[shock_name][resp_name] = [
                    float(irfs[h][resp_idx, shock_idx]) for h in range(irf_horizon)
                ]

        results["irf"] = irf_formatted

        # Bootstrap confidence bands
        if n_bootstrap > 0:
            ci = self._bootstrap_irf_ci(
                Y, var_lags, irf_horizon, n_bootstrap, available_names
            )
            results["irf_confidence"] = ci

        # FEVD
        fevd = self._compute_fevd(irfs, irf_horizon)
        fevd_formatted = {}
        for resp_idx, resp_name in enumerate(available_names):
            fevd_formatted[resp_name] = {}
            for shock_idx, shock_name in enumerate(available_names):
                fevd_formatted[resp_name][shock_name] = [
                    float(fevd[h][resp_idx, shock_idx]) for h in range(irf_horizon)
                ]

        results["fevd"] = fevd_formatted

        # Historical decomposition
        hd = self._historical_decomposition(Y, B, k, var_lags, P, available_names)
        results["historical_decomposition"] = hd

        # Granger causality tests
        granger = self._granger_causality(Y, var_lags, available_names)
        results["granger_causality"] = granger

        # Forecast
        forecast = self._var_forecast(Y, B, k, var_lags, horizon=8)
        results["forecast"] = {
            "horizon": 8,
            "values": {
                name: [float(forecast[h, i]) for h in range(8)]
                for i, name in enumerate(available_names)
            },
        }

        # Score: based on recent forecast variance and shock persistence
        # High forecast uncertainty or persistent negative GDP shocks = stress
        gdp_idx = available_names.index("gdp") if "gdp" in available_names else 0
        gdp_forecast = forecast[:, gdp_idx]
        if np.mean(gdp_forecast) < 0:
            score = float(np.clip(50.0 + abs(np.mean(gdp_forecast)) * 15.0, 0, 100))
        else:
            score = float(np.clip(50.0 - np.mean(gdp_forecast) * 10.0, 0, 100))

        return {
            "score": score,
            "results": results,
        }

    @staticmethod
    def _transform_data(raw: np.ndarray, names: list[str]) -> tuple:
        """Transform raw data: log-difference for quantity/price variables, levels for rates."""
        n, k = raw.shape
        transforms = {}
        columns = []

        for i, name in enumerate(names):
            col = raw[:, i]
            if name in ("fed_funds",):
                # Keep in levels (already in percentage points)
                columns.append(col)
                transforms[name] = "level"
            else:
                # Log-difference, annualized monthly growth
                with np.errstate(divide="ignore", invalid="ignore"):
                    log_col = np.log(np.maximum(col, 1e-12))
                diff = np.diff(log_col) * 1200  # annualized
                columns.append(diff)
                transforms[name] = "log_diff_annualized"

        # Align: find minimum length
        min_len = min(len(c) for c in columns)
        Y = np.column_stack([c[-min_len:] for c in columns])

        # Remove any NaN/inf rows
        valid = np.all(np.isfinite(Y), axis=1)
        Y = Y[valid]

        return Y, transforms

    @staticmethod
    def _estimate_var(Y: np.ndarray, lags: int) -> tuple | None:
        """Estimate VAR(p) via OLS. Returns (B, Sigma, residuals, n_obs)."""
        T, k = Y.shape
        if T <= lags + k:
            return None

        # Build lagged matrix
        Y_dep = Y[lags:]
        n_obs = Y_dep.shape[0]

        X = np.ones((n_obs, 1))  # constant
        for lag in range(1, lags + 1):
            X = np.hstack([X, Y[lags - lag:T - lag]])

        # OLS
        try:
            XtX_inv = np.linalg.inv(X.T @ X)
        except np.linalg.LinAlgError:
            return None

        B = XtX_inv @ X.T @ Y_dep
        residuals = Y_dep - X @ B
        Sigma = (residuals.T @ residuals) / (n_obs - X.shape[1])

        return B, Sigma, residuals, n_obs

    @staticmethod
    def _compute_irf(B: np.ndarray, k: int, p: int,
                     P: np.ndarray, horizon: int) -> list[np.ndarray]:
        """Compute structural IRFs via Cholesky decomposition."""
        # Extract VAR coefficient matrices (skip constant row)
        A_mats = []
        for lag in range(p):
            A_mats.append(B[1 + lag * k:1 + (lag + 1) * k, :].T)

        # Companion form
        kp = k * p
        F = np.zeros((kp, kp))
        for lag in range(p):
            F[:k, lag * k:(lag + 1) * k] = A_mats[lag]
        if p > 1:
            F[k:, :k * (p - 1)] = np.eye(k * (p - 1))

        J = np.zeros((k, kp))
        J[:k, :k] = np.eye(k)

        irfs = []
        F_power = np.eye(kp)
        for h in range(horizon):
            Phi_h = J @ F_power @ J.T @ P
            irfs.append(Phi_h)
            F_power = F_power @ F

        return irfs

    @staticmethod
    def _compute_fevd(irfs: list[np.ndarray], horizon: int) -> list[np.ndarray]:
        """Forecast error variance decomposition."""
        k = irfs[0].shape[0]
        fevd = []
        cum_var = np.zeros((k, k))

        for h in range(horizon):
            cum_var += irfs[h] ** 2
            total = cum_var.sum(axis=1, keepdims=True)
            total[total < 1e-12] = 1.0
            fevd.append(cum_var / total)

        return fevd

    def _bootstrap_irf_ci(self, Y: np.ndarray, lags: int,
                          horizon: int, n_boot: int,
                          names: list[str]) -> dict:
        """Bootstrap confidence intervals for IRFs (percentile method)."""
        k = Y.shape[1]
        all_irfs = np.zeros((n_boot, horizon, k, k))

        # Original estimation
        var_est = self._estimate_var(Y, lags)
        if var_est is None:
            return {}

        B_orig, Sigma_orig, resid_orig, n_obs = var_est

        for b in range(n_boot):
            # Resample residuals with replacement
            boot_idx = np.random.randint(0, n_obs, size=n_obs)
            boot_resid = resid_orig[boot_idx]

            # Generate bootstrap data
            Y_boot = np.zeros_like(Y)
            Y_boot[:lags] = Y[:lags]

            X_row = np.ones(1 + k * lags)
            for t in range(lags, len(Y)):
                X_row[0] = 1.0
                for lag in range(lags):
                    X_row[1 + lag * k:1 + (lag + 1) * k] = Y_boot[t - lag - 1]
                Y_boot[t] = X_row @ B_orig + boot_resid[min(t - lags, n_obs - 1)]

            # Re-estimate
            boot_est = self._estimate_var(Y_boot, lags)
            if boot_est is None:
                continue

            B_boot, Sigma_boot, _, _ = boot_est
            try:
                P_boot = np.linalg.cholesky(Sigma_boot)
            except np.linalg.LinAlgError:
                continue

            irfs_boot = self._compute_irf(B_boot, k, lags, P_boot, horizon)
            for h in range(horizon):
                all_irfs[b, h] = irfs_boot[h]

        # Compute percentile CIs
        ci = {}
        for shock_idx, shock_name in enumerate(names):
            ci[shock_name] = {}
            for resp_idx, resp_name in enumerate(names):
                vals = all_irfs[:, :, resp_idx, shock_idx]
                ci[shock_name][resp_name] = {
                    "lower_16": [float(np.percentile(vals[:, h], 16)) for h in range(horizon)],
                    "upper_84": [float(np.percentile(vals[:, h], 84)) for h in range(horizon)],
                    "lower_5": [float(np.percentile(vals[:, h], 5)) for h in range(horizon)],
                    "upper_95": [float(np.percentile(vals[:, h], 95)) for h in range(horizon)],
                }

        return ci

    def _historical_decomposition(self, Y: np.ndarray, B: np.ndarray,
                                  k: int, lags: int, P: np.ndarray,
                                  names: list[str]) -> dict:
        """Decompose each variable into contributions from each structural shock."""
        T = Y.shape[0]
        n_obs = T - lags

        # Get structural shocks: e_t = P^{-1} @ u_t
        Y_dep = Y[lags:]
        X = np.ones((n_obs, 1))
        for lag in range(1, lags + 1):
            X = np.hstack([X, Y[lags - lag:T - lag]])

        residuals = Y_dep - X @ B
        P_inv = np.linalg.inv(P)
        structural_shocks = residuals @ P_inv.T

        # Contribution of each shock to each variable (last 24 observations)
        n_show = min(24, n_obs)
        contributions = {}
        for resp_idx, resp_name in enumerate(names):
            contributions[resp_name] = {}
            for shock_idx, shock_name in enumerate(names):
                # Simplified: contemporaneous contribution
                contribs = structural_shocks[-n_show:, shock_idx] * P[resp_idx, shock_idx]
                contributions[resp_name][shock_name] = [float(c) for c in contribs]

        return {"n_periods": n_show, "contributions": contributions}

    def _granger_causality(self, Y: np.ndarray, lags: int,
                           names: list[str]) -> dict:
        """Pairwise Granger causality tests."""
        k = Y.shape[1]
        T = Y.shape[0]
        results = {}

        # Full VAR
        var_full = self._estimate_var(Y, lags)
        if var_full is None:
            return {}

        _, _, resid_full, n_obs = var_full
        np.sum(resid_full ** 2, axis=0)

        for cause_idx, cause_name in enumerate(names):
            for effect_idx, effect_name in enumerate(names):
                if cause_idx == effect_idx:
                    continue

                # Restricted VAR: exclude lags of cause variable from effect equation
                # Build restricted Y excluding cause variable lags
                Y_dep = Y[lags:, effect_idx]

                # Unrestricted X
                X_u = np.ones((n_obs, 1))
                for lag in range(1, lags + 1):
                    X_u = np.hstack([X_u, Y[lags - lag:T - lag]])

                # Restricted X: drop cause variable lags
                cols_to_drop = [1 + lag * k + cause_idx for lag in range(lags)]
                mask = np.ones(X_u.shape[1], dtype=bool)
                mask[cols_to_drop] = False
                X_r = X_u[:, mask]

                # OLS for restricted
                beta_r = np.linalg.lstsq(X_r, Y_dep, rcond=None)[0]
                resid_r = Y_dep - X_r @ beta_r
                rss_r = float(np.sum(resid_r ** 2))

                # OLS for unrestricted
                beta_u = np.linalg.lstsq(X_u, Y_dep, rcond=None)[0]
                resid_u = Y_dep - X_u @ beta_u
                rss_u = float(np.sum(resid_u ** 2))

                # F-test
                df1 = lags  # number of restrictions
                df2 = n_obs - X_u.shape[1]

                if rss_u < 1e-12:
                    f_stat = 0.0
                    p_value = 1.0
                else:
                    f_stat = ((rss_r - rss_u) / df1) / (rss_u / df2)
                    p_value = 1 - sp_stats.f.cdf(f_stat, df1, df2)

                results[f"{cause_name} -> {effect_name}"] = {
                    "f_statistic": float(f_stat),
                    "p_value": float(p_value),
                    "granger_causes": p_value < 0.05,
                }

        return results

    @staticmethod
    def _var_forecast(Y: np.ndarray, B: np.ndarray, k: int,
                      lags: int, horizon: int) -> np.ndarray:
        """Generate h-step ahead VAR forecasts."""
        Y.shape[0]
        forecasts = np.zeros((horizon, k))

        # Last lags observations for initial conditions
        history = Y[-lags:].copy()

        for h in range(horizon):
            x = np.ones(1 + k * lags)
            for lag in range(lags):
                if h - lag - 1 >= 0:
                    x[1 + lag * k:1 + (lag + 1) * k] = forecasts[h - lag - 1]
                else:
                    x[1 + lag * k:1 + (lag + 1) * k] = history[-(lag + 1 - h)]

            forecasts[h] = x @ B

        return forecasts

    @staticmethod
    def _information_criteria(residuals: np.ndarray, k: int,
                              p: int, n: int) -> tuple[float, float]:
        """AIC and BIC for VAR model."""
        Sigma = (residuals.T @ residuals) / n
        det_sigma = np.linalg.det(Sigma)
        if det_sigma <= 0:
            det_sigma = 1e-12

        log_det = np.log(det_sigma)
        n_params = k * (1 + k * p)  # constant + k*p coefficients per equation

        aic = log_det + 2 * n_params / n
        bic = log_det + np.log(n) * n_params / n

        return float(aic), float(bic)

    def _select_lags(self, Y: np.ndarray, max_lags: int = 8) -> dict:
        """Select optimal lag order by AIC and BIC."""
        best_aic = (float("inf"), 1)
        best_bic = (float("inf"), 1)

        for p in range(1, max_lags + 1):
            est = self._estimate_var(Y, p)
            if est is None:
                continue
            _, _, resid, n_obs = est
            k = Y.shape[1]
            aic, bic = self._information_criteria(resid, k, p, n_obs)

            if aic < best_aic[0]:
                best_aic = (aic, p)
            if bic < best_bic[0]:
                best_bic = (bic, p)

        return {"aic": best_aic[1], "bic": best_bic[1]}
