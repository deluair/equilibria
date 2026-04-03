"""Spatial econometrics: SAR, SEM, SDM models (Anselin 1988, LeSage & Pace 2009).

Spatial econometric models account for spatial dependence in cross-sectional
or panel data. Units (regions, countries) are linked through a spatial weights
matrix W that encodes proximity (contiguity, inverse distance, k-nearest).

Three canonical models:
    SAR (Spatial Autoregressive): y = rho * W * y + X * beta + e
        Endogenous spatial lag. Interpretation: neighbor outcomes affect own.
    SEM (Spatial Error Model): y = X * beta + u, u = lambda * W * u + e
        Spatial correlation in errors. Interpretation: omitted spatially
        correlated factors.
    SDM (Spatial Durbin Model): y = rho * W * y + X * beta + W * X * theta + e
        Both spatial lag and spatially lagged covariates. Nests SAR and SEM
        as special cases (LeSage & Pace 2009).

Key implementation:
    1. Spatial weights construction (contiguity, distance threshold, k-nn)
    2. Row-standardization of W
    3. LM test statistics for spatial lag vs error dependence
    4. ML or GMM estimation of SAR, SEM, SDM
    5. Direct/indirect/total effects decomposition for SDM

References:
    Anselin, L. (1988). Spatial Econometrics: Methods and Models. Kluwer.
    LeSage, J. & Pace, R.K. (2009). Introduction to Spatial Econometrics.
        CRC Press.
    Anselin, L., Bera, A., Florax, R. & Yoon, M. (1996). Simple Diagnostic
        Tests for Spatial Dependence. Regional Science and Urban Economics
        26(1): 77-104.

Score: strong spatial dependence (high rho/lambda, significant LM tests) ->
high score (spatial structure matters). Weak dependence -> STABLE.
"""

import json

import numpy as np

from app.layers.base import LayerBase


class SpatialEconometrics(LayerBase):
    layer_id = "l18"
    name = "Spatial Econometrics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        weights_type = kwargs.get("weights_type", "contiguity")

        rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata, ds.country_iso3
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'spatial_econ'
            ORDER BY ds.country_iso3
            """,
            (),
        )

        if not rows or len(rows) < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient spatial data"}

        # Parse cross-sectional data: y, X, and spatial linkages
        units = {}
        for row in rows:
            iso3 = row["country_iso3"]
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            y_val = row["value"]
            if y_val is None:
                continue
            x_vars = meta.get("covariates", {})
            neighbors = meta.get("neighbors", [])
            coords = meta.get("coordinates")
            units.setdefault(iso3, {
                "y": float(y_val),
                "x": x_vars,
                "neighbors": neighbors,
                "coords": coords,
            })

        unit_names = sorted(units.keys())
        n = len(unit_names)
        if n < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "need >= 5 spatial units"}

        idx = {name: i for i, name in enumerate(unit_names)}
        y = np.array([units[u]["y"] for u in unit_names])

        # Build X matrix from union of all covariate keys
        all_x_keys = set()
        for u in unit_names:
            all_x_keys |= set(units[u]["x"].keys())
        x_keys = sorted(all_x_keys)
        if not x_keys:
            X = np.ones((n, 1))
            x_keys = ["constant"]
        else:
            X = np.column_stack([
                np.ones(n),
                *[np.array([units[u]["x"].get(k, 0.0) for u in unit_names]) for k in x_keys],
            ])
            x_keys = ["constant"] + x_keys

        # Build spatial weights matrix
        W = self._build_weights(units, unit_names, idx, weights_type)

        # Row-standardize W
        row_sums = W.sum(axis=1)
        row_sums[row_sums == 0] = 1.0
        W_rs = W / row_sums[:, None]

        # OLS baseline
        ols = self._ols(X, y)

        # LM tests for spatial dependence (Anselin et al. 1996)
        lm_lag, lm_err, lm_lag_robust, lm_err_robust = self._lm_tests(X, y, W_rs, ols["residuals"])

        # SAR estimation via concentrated ML
        sar = self._estimate_sar(X, y, W_rs)

        # SEM estimation via concentrated ML
        sem = self._estimate_sem(X, y, W_rs)

        # SDM estimation
        sdm = self._estimate_sdm(X, y, W_rs)

        # Score: strong spatial dependence -> high score
        max_lm = max(lm_lag, lm_err)
        rho_abs = abs(sar["rho"])
        lambda_abs = abs(sem["lambda_"])

        if max_lm > 10 or rho_abs > 0.5 or lambda_abs > 0.5:
            score = 60.0 + min(max_lm / 5.0, 40.0)
        elif max_lm > 3.84:  # chi2(1) 5% critical value
            score = 30.0 + (max_lm - 3.84) * 4.87
        else:
            score = max_lm * 7.8
        score = max(0.0, min(100.0, score))

        return {
            "score": round(score, 2),
            "country": country,
            "n_units": n,
            "weights_type": weights_type,
            "ols_baseline": {
                "coefficients": {k: round(float(v), 4) for k, v in zip(x_keys, ols["beta"])},
                "r_squared": round(ols["r2"], 4),
            },
            "lm_tests": {
                "lm_lag": round(lm_lag, 4),
                "lm_error": round(lm_err, 4),
                "lm_lag_robust": round(lm_lag_robust, 4),
                "lm_error_robust": round(lm_err_robust, 4),
            },
            "sar": {
                "rho": round(sar["rho"], 4),
                "log_likelihood": round(sar["ll"], 4),
            },
            "sem": {
                "lambda": round(sem["lambda_"], 4),
                "log_likelihood": round(sem["ll"], 4),
            },
            "sdm": {
                "rho": round(sdm["rho"], 4),
                "log_likelihood": round(sdm["ll"], 4),
            },
            "model_selection": {
                "preferred": self._select_model(lm_lag, lm_err, lm_lag_robust, lm_err_robust),
            },
        }

    @staticmethod
    def _build_weights(units: dict, unit_names: list, idx: dict,
                       weights_type: str) -> np.ndarray:
        """Build spatial weights matrix."""
        n = len(unit_names)
        W = np.zeros((n, n))

        if weights_type == "contiguity":
            for u in unit_names:
                i = idx[u]
                for nb in units[u].get("neighbors", []):
                    if nb in idx:
                        j = idx[nb]
                        W[i, j] = 1.0
        elif weights_type == "distance":
            for i, u1 in enumerate(unit_names):
                c1 = units[u1].get("coords")
                if c1 is None:
                    continue
                for j, u2 in enumerate(unit_names):
                    if i == j:
                        continue
                    c2 = units[u2].get("coords")
                    if c2 is None:
                        continue
                    dist = np.sqrt((c1[0] - c2[0]) ** 2 + (c1[1] - c2[1]) ** 2)
                    if dist > 0:
                        W[i, j] = 1.0 / dist
        else:
            # k-nearest neighbors (k=5)
            k = 5
            for i, u1 in enumerate(unit_names):
                c1 = units[u1].get("coords")
                if c1 is None:
                    # Fallback to contiguity
                    for nb in units[u1].get("neighbors", []):
                        if nb in idx:
                            W[i, idx[nb]] = 1.0
                    continue
                dists = []
                for j, u2 in enumerate(unit_names):
                    if i == j:
                        dists.append(float("inf"))
                        continue
                    c2 = units[u2].get("coords")
                    if c2 is None:
                        dists.append(float("inf"))
                    else:
                        dists.append(np.sqrt((c1[0] - c2[0]) ** 2 + (c1[1] - c2[1]) ** 2))
                nearest = np.argsort(dists)[:k]
                for j in nearest:
                    if dists[j] < float("inf"):
                        W[i, j] = 1.0
        return W

    @staticmethod
    def _ols(X: np.ndarray, y: np.ndarray) -> dict:
        """OLS with residuals."""
        beta = np.linalg.lstsq(X, y, rcond=None)[0]
        resid = y - X @ beta
        ss_res = float(np.sum(resid ** 2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
        return {"beta": beta, "residuals": resid, "r2": r2, "sigma2": ss_res / len(y)}

    @staticmethod
    def _lm_tests(X: np.ndarray, y: np.ndarray, W: np.ndarray,
                  e: np.ndarray) -> tuple:
        """Lagrange Multiplier tests for spatial lag and error (Anselin 1996)."""
        n = len(y)
        sigma2 = float(np.sum(e ** 2)) / n
        We = W @ e
        Wy = W @ y

        # Projection matrix
        try:
            M = np.eye(n) - X @ np.linalg.inv(X.T @ X) @ X.T
        except np.linalg.LinAlgError:
            M = np.eye(n) - X @ np.linalg.pinv(X.T @ X) @ X.T

        # Trace terms
        T = np.trace(W @ W + W.T @ W)

        # LM-Lag: (e'Wy / sigma2)^2 / (((MWXb)'(MWXb)/sigma2) + T)
        # Simplified: (e'Wy)^2 / (sigma2 * n_wy)
        eWy = float(e @ Wy)
        eWe = float(e @ We)

        # Denominator for LM-lag
        MWXb = M @ W @ (X @ np.linalg.lstsq(X, y, rcond=None)[0])
        nwy = float(MWXb @ MWXb) / sigma2 + T

        lm_lag = (eWy / sigma2) ** 2 / nwy if nwy > 0 else 0.0

        # LM-Error: (e'We / sigma2)^2 / T
        lm_err = (eWe / sigma2) ** 2 / T if T > 0 else 0.0

        # Robust versions
        lm_err_robust = max(0.0, (eWe / sigma2 - T * eWy / (sigma2 * nwy)) ** 2
                           / (T - T ** 2 / nwy)) if (T - T ** 2 / nwy) > 0 else 0.0
        lm_lag_robust = max(0.0, (eWy / sigma2 - eWe / sigma2) ** 2
                           / (nwy - T)) if (nwy - T) > 0 else 0.0

        return lm_lag, lm_err, lm_lag_robust, lm_err_robust

    @staticmethod
    def _estimate_sar(X: np.ndarray, y: np.ndarray, W: np.ndarray) -> dict:
        """SAR estimation via concentrated ML over rho grid."""
        n = len(y)
        best_ll = -np.inf
        best_rho = 0.0

        for rho in np.linspace(-0.9, 0.9, 37):
            y_star = y - rho * (W @ y)
            beta = np.linalg.lstsq(X, y_star, rcond=None)[0]
            resid = y_star - X @ beta
            sigma2 = float(np.sum(resid ** 2)) / n
            if sigma2 <= 0:
                continue
            ll = -n / 2.0 * np.log(2 * np.pi * sigma2) - n / 2.0
            # Log-determinant of (I - rho*W) via eigenvalues
            try:
                eigvals = np.linalg.eigvalsh(np.eye(n) - rho * W)
                eigvals = eigvals[eigvals > 0]
                if len(eigvals) == n:
                    ll += float(np.sum(np.log(eigvals)))
                else:
                    continue
            except np.linalg.LinAlgError:
                continue
            if ll > best_ll:
                best_ll = ll
                best_rho = rho

        return {"rho": float(best_rho), "ll": float(best_ll)}

    @staticmethod
    def _estimate_sem(X: np.ndarray, y: np.ndarray, W: np.ndarray) -> dict:
        """SEM estimation via concentrated ML over lambda grid."""
        n = len(y)
        best_ll = -np.inf
        best_lam = 0.0

        for lam in np.linspace(-0.9, 0.9, 37):
            A = np.eye(n) - lam * W
            y_star = A @ y
            X_star = A @ X
            beta = np.linalg.lstsq(X_star, y_star, rcond=None)[0]
            resid = y_star - X_star @ beta
            sigma2 = float(np.sum(resid ** 2)) / n
            if sigma2 <= 0:
                continue
            ll = -n / 2.0 * np.log(2 * np.pi * sigma2) - n / 2.0
            try:
                eigvals = np.linalg.eigvalsh(A)
                eigvals = eigvals[eigvals > 0]
                if len(eigvals) == n:
                    ll += float(np.sum(np.log(eigvals)))
                else:
                    continue
            except np.linalg.LinAlgError:
                continue
            if ll > best_ll:
                best_ll = ll
                best_lam = lam

        return {"lambda_": float(best_lam), "ll": float(best_ll)}

    @staticmethod
    def _estimate_sdm(X: np.ndarray, y: np.ndarray, W: np.ndarray) -> dict:
        """SDM: SAR with spatially lagged X."""
        n, k = X.shape
        WX = W @ X
        X_sdm = np.column_stack([X, WX])

        best_ll = -np.inf
        best_rho = 0.0

        for rho in np.linspace(-0.9, 0.9, 37):
            y_star = y - rho * (W @ y)
            beta = np.linalg.lstsq(X_sdm, y_star, rcond=None)[0]
            resid = y_star - X_sdm @ beta
            sigma2 = float(np.sum(resid ** 2)) / n
            if sigma2 <= 0:
                continue
            ll = -n / 2.0 * np.log(2 * np.pi * sigma2) - n / 2.0
            try:
                eigvals = np.linalg.eigvalsh(np.eye(n) - rho * W)
                eigvals = eigvals[eigvals > 0]
                if len(eigvals) == n:
                    ll += float(np.sum(np.log(eigvals)))
                else:
                    continue
            except np.linalg.LinAlgError:
                continue
            if ll > best_ll:
                best_ll = ll
                best_rho = rho

        return {"rho": float(best_rho), "ll": float(best_ll)}

    @staticmethod
    def _select_model(lm_lag: float, lm_err: float,
                      lm_lag_r: float, lm_err_r: float) -> str:
        """Anselin (2005) decision rule for spatial model selection."""
        chi2_5 = 3.84
        if lm_lag < chi2_5 and lm_err < chi2_5:
            return "OLS"
        if lm_lag > lm_err:
            if lm_lag_r > chi2_5 and lm_err_r <= chi2_5:
                return "SAR"
            elif lm_lag_r > chi2_5 and lm_err_r > chi2_5:
                return "SDM"
            else:
                return "SAR"
        else:
            if lm_err_r > chi2_5 and lm_lag_r <= chi2_5:
                return "SEM"
            elif lm_err_r > chi2_5 and lm_lag_r > chi2_5:
                return "SDM"
            else:
                return "SEM"
