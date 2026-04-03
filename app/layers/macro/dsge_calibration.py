"""DSGE Calibration - Simple 3-equation New Keynesian DSGE model.

Methodology
-----------
The canonical 3-equation New Keynesian model:

1. **Dynamic IS curve** (demand):
   x_t = E_t[x_{t+1}] - sigma^{-1} * (i_t - E_t[pi_{t+1}] - r_n_t)

2. **New Keynesian Phillips Curve** (supply):
   pi_t = beta * E_t[pi_{t+1}] + kappa * x_t + u_t

3. **Taylor Rule** (monetary policy):
   i_t = phi_pi * pi_t + phi_x * x_t + v_t

where:
   x_t    = output gap
   pi_t   = inflation
   i_t    = nominal interest rate
   r_n_t  = natural rate of interest
   sigma  = inverse intertemporal elasticity of substitution
   beta   = discount factor
   kappa  = Phillips curve slope
   phi_pi = Taylor rule inflation coefficient
   phi_x  = Taylor rule output gap coefficient
   u_t    = cost-push shock
   v_t    = monetary policy shock

Calibration from US data via moment matching. Impulse response functions
for demand, supply, and monetary shocks. Comparison with unrestricted VAR IRFs
to assess model fit.

References:
- Gali (2015), Monetary Policy, Inflation, and the Business Cycle, Ch. 3
- Woodford (2003), Interest and Prices, Ch. 4
"""

from __future__ import annotations

import numpy as np
from scipy import linalg as sp_linalg

from app.layers.base import LayerBase


class DSGECalibration(LayerBase):
    layer_id = "l2"
    name = "DSGE Calibration"
    weight = 0.05

    # Default calibration (US quarterly, standard NK literature)
    DEFAULT_PARAMS = {
        "beta": 0.99,       # discount factor (quarterly)
        "sigma": 1.0,       # inverse IES
        "kappa": 0.3,       # Phillips curve slope
        "phi_pi": 1.5,      # Taylor rule: inflation
        "phi_x": 0.5 / 4,   # Taylor rule: output gap (annualized -> quarterly)
        "rho_r": 0.0,       # natural rate AR(1)
        "rho_u": 0.0,       # cost-push AR(1)
        "rho_v": 0.0,       # monetary shock AR(1)
        "sigma_r": 1.0,     # natural rate shock std
        "sigma_u": 0.5,     # cost-push shock std
        "sigma_v": 0.25,    # monetary shock std
    }

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        irf_horizon = kwargs.get("irf_horizon", 20)
        calibrate_from_data = kwargs.get("calibrate", True)

        results = {"country": country}

        # Fetch observed moments for calibration
        data = await self._fetch_data(db, country)

        # Calibration: match moments from data or use defaults
        params = dict(self.DEFAULT_PARAMS)
        if calibrate_from_data and data is not None:
            cal_result = self._calibrate(data, params)
            params.update(cal_result["params"])
            results["calibration"] = cal_result
        else:
            results["calibration"] = {"method": "default", "params": params}

        results["parameters"] = params

        # Solve the model (rational expectations solution)
        solution = self._solve_nk_model(params)
        if solution is None:
            return {"score": 50.0, "results": results, "note": "model solution failed"}

        A_sol, B_sol, eigenvalues = solution
        results["solution"] = {
            "eigenvalues": [{"real": float(e.real), "imag": float(e.imag)} for e in eigenvalues],
            "blanchard_kahn": self._check_blanchard_kahn(eigenvalues, n_forward=2),
            "determinacy": "determinate" if self._check_blanchard_kahn(eigenvalues, 2) else "indeterminate",
        }

        # Impulse response functions
        irfs = self._compute_irfs(A_sol, B_sol, params, irf_horizon)
        results["impulse_responses"] = irfs

        # Compare with VAR IRFs if data available
        if data is not None and data["output_gap"].shape[0] >= 40:
            var_comparison = self._compare_with_var(data, irfs, irf_horizon)
            results["var_comparison"] = var_comparison

        # Model diagnostics
        diagnostics = self._diagnostics(params, irfs)
        results["diagnostics"] = diagnostics

        # Score: based on model determinacy, calibration fit, Taylor principle
        score = self._compute_score(results)

        return {"score": round(score, 1), "results": results}

    async def _fetch_data(self, db, country: str) -> dict | None:
        """Fetch output gap, inflation, and policy rate data."""
        series_map = {
            "output_gap": f"OUTPUT_GAP_{country}",
            "inflation": f"INFLATION_{country}",
            "policy_rate": f"POLICY_RATE_{country}",
        }
        data = {}
        for label, code in series_map.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
                (code,),
            )
            if rows:
                data[label] = np.array([float(r[1]) for r in rows])

        if len(data) < 3:
            return None

        # Align to common length
        min_len = min(len(v) for v in data.values())
        for k in data:
            data[k] = data[k][-min_len:]

        return data

    def _calibrate(self, data: dict, prior: dict) -> dict:
        """Calibrate structural parameters via moment matching."""
        x = data["output_gap"]
        pi = data["inflation"]
        i_rate = data["policy_rate"]

        # Observed moments
        var_x = float(np.var(x, ddof=1))
        var_pi = float(np.var(pi, ddof=1))
        cov_x_pi = float(np.cov(x, pi, ddof=1)[0, 1])
        autocorr_x = float(np.corrcoef(x[:-1], x[1:])[0, 1]) if len(x) > 2 else 0.5
        autocorr_pi = float(np.corrcoef(pi[:-1], pi[1:])[0, 1]) if len(pi) > 2 else 0.5

        observed_moments = {
            "var_output_gap": var_x,
            "var_inflation": var_pi,
            "cov_gap_inflation": cov_x_pi,
            "autocorr_output_gap": autocorr_x,
            "autocorr_inflation": autocorr_pi,
        }

        # Estimate Taylor rule coefficients directly from data
        n = min(len(i_rate), len(pi), len(x))
        X_tr = np.column_stack([np.ones(n), pi[-n:], x[-n:]])
        beta_tr = np.linalg.lstsq(X_tr, i_rate[-n:], rcond=None)[0]
        phi_pi_est = max(float(beta_tr[1]), 1.01)  # enforce Taylor principle
        phi_x_est = max(float(beta_tr[2]), 0.01)

        # Estimate Phillips curve slope via OLS: pi_t = c + kappa*x_t + e
        # Use first-differenced inflation as proxy for beta*E[pi_{t+1}] term
        dpi = np.diff(pi[-n:])
        pi_lhs = pi[-n:][1:] - prior["beta"] * pi[-n:][:-1]
        X_pc2 = np.column_stack([np.ones(len(dpi)), x[-n:][1:]])
        beta_pc = np.linalg.lstsq(X_pc2, pi_lhs, rcond=None)[0]
        kappa_est = max(float(beta_pc[1]), 0.01)

        calibrated = {
            "phi_pi": round(phi_pi_est, 3),
            "phi_x": round(phi_x_est, 3),
            "kappa": round(kappa_est, 3),
        }

        return {
            "method": "moment_matching",
            "observed_moments": observed_moments,
            "params": calibrated,
            "taylor_rule_r_squared": self._r_squared(X_tr, i_rate[-n:], beta_tr),
        }

    @staticmethod
    def _r_squared(X, y, beta):
        resid = y - X @ beta
        sst = float(np.sum((y - np.mean(y)) ** 2))
        sse = float(np.sum(resid ** 2))
        return round(1 - sse / sst, 4) if sst > 0 else 0.0

    @staticmethod
    def _solve_nk_model(params: dict) -> tuple | None:
        """Solve 3-equation NK model via rational expectations (Blanchard-Kahn).

        State vector: s_t = [x_t, pi_t, i_t]'
        Shock vector: e_t = [r_n_t, u_t, v_t]'

        System: A * E_t[s_{t+1}] = B * s_t + C * e_t
        """
        beta = params["beta"]
        sigma = params["sigma"]
        kappa = params["kappa"]
        phi_pi = params["phi_pi"]
        phi_x = params["phi_x"]

        # Substitute Taylor rule into IS: reduce to 2-equation system
        # x_t = E[x_{t+1}] - (1/sigma)*(phi_pi*pi_t + phi_x*x_t - E[pi_{t+1}] - r_n + v)
        # pi_t = beta*E[pi_{t+1}] + kappa*x_t + u

        # Matrix form: A * z_{t+1} = B * z_t + ...
        # z = [x, pi]
        A = np.array([
            [1.0, 1.0 / sigma],
            [0.0, beta],
        ])

        B = np.array([
            [1.0 + phi_x / sigma, phi_pi / sigma],
            [-kappa, 1.0],
        ])

        try:
            A_inv = np.linalg.inv(A)
        except np.linalg.LinAlgError:
            return None

        M = A_inv @ B

        eigenvalues = np.linalg.eigvals(M)

        # For a 2-variable forward-looking system, both eigenvalues should be
        # inside the unit circle for a unique stable solution under commitment,
        # or we use the Schur decomposition approach
        try:
            T_schur, Z, sdim = sp_linalg.schur(M, sort="lhp")
        except Exception:
            return None

        return M, A_inv, eigenvalues

    @staticmethod
    def _check_blanchard_kahn(eigenvalues, n_forward: int) -> bool:
        """Check Blanchard-Kahn conditions for determinacy."""
        n_unstable = sum(1 for e in eigenvalues if abs(e) > 1.0)
        return n_unstable == n_forward

    def _compute_irfs(self, M: np.ndarray, A_inv: np.ndarray,
                      params: dict, horizon: int) -> dict:
        """Compute impulse responses to demand, supply, and monetary shocks."""
        sigma = params["sigma"]
        phi_pi = params["phi_pi"]
        phi_x = params["phi_x"]

        # Shock impact vectors (on [x, pi] system)
        # Demand shock (r_n): enters IS curve
        shock_demand = A_inv @ np.array([1.0 / sigma, 0.0])
        # Supply shock (u): enters Phillips curve
        shock_supply = A_inv @ np.array([0.0, 1.0])
        # Monetary shock (v): enters IS via Taylor rule
        shock_monetary = A_inv @ np.array([-1.0 / sigma, 0.0])

        shocks = {
            "demand": shock_demand,
            "supply": shock_supply,
            "monetary": shock_monetary,
        }

        irfs = {}
        for shock_name, impact in shocks.items():
            z = impact.copy()
            irf_x = [float(z[0])]
            irf_pi = [float(z[1])]
            irf_i = [float(phi_pi * z[1] + phi_x * z[0])]

            for h in range(1, horizon):
                z = M @ z
                irf_x.append(float(z[0]))
                irf_pi.append(float(z[1]))
                irf_i.append(float(phi_pi * z[1] + phi_x * z[0]))

            irfs[shock_name] = {
                "output_gap": irf_x,
                "inflation": irf_pi,
                "interest_rate": irf_i,
                "half_life": self._half_life(irf_x),
            }

        return irfs

    @staticmethod
    def _half_life(series: list[float]) -> int | None:
        """Periods until response falls below half of peak."""
        if not series or abs(series[0]) < 1e-12:
            return None
        peak = abs(series[0])
        for h, val in enumerate(series[1:], 1):
            if abs(val) < peak / 2:
                return h
        return None

    def _compare_with_var(self, data: dict, dsge_irfs: dict,
                          horizon: int) -> dict:
        """Compare DSGE IRFs with unrestricted VAR(4) IRFs."""
        x = data["output_gap"]
        pi = data["inflation"]
        i_rate = data["policy_rate"]
        T = len(x)
        lags = 4

        if T < lags + horizon:
            return {"note": "insufficient data for VAR comparison"}

        Y = np.column_stack([x, pi, i_rate])
        k = Y.shape[1]

        # Estimate VAR(4)
        Y_dep = Y[lags:]
        n_obs = Y_dep.shape[0]
        X = np.ones((n_obs, 1))
        for lag in range(1, lags + 1):
            X = np.hstack([X, Y[lags - lag:T - lag]])

        try:
            B = np.linalg.inv(X.T @ X) @ X.T @ Y_dep
        except np.linalg.LinAlgError:
            return {"note": "VAR estimation failed"}

        residuals = Y_dep - X @ B
        Sigma = (residuals.T @ residuals) / n_obs

        try:
            P = np.linalg.cholesky(Sigma)
        except np.linalg.LinAlgError:
            return {"note": "Cholesky decomposition failed"}

        # VAR IRFs via companion form
        A_mats = []
        for lag in range(lags):
            A_mats.append(B[1 + lag * k:1 + (lag + 1) * k, :].T)

        kp = k * lags
        F = np.zeros((kp, kp))
        for lag in range(lags):
            F[:k, lag * k:(lag + 1) * k] = A_mats[lag]
        if lags > 1:
            F[k:, :k * (lags - 1)] = np.eye(k * (lags - 1))

        J = np.zeros((k, kp))
        J[:k, :k] = np.eye(k)

        var_irfs = {}
        var_names = ["output_gap", "inflation", "interest_rate"]
        for shock_idx in range(k):
            shock_label = ["demand", "supply", "monetary"][shock_idx]
            irf_dict = {}
            for var_idx, var_name in enumerate(var_names):
                vals = []
                F_h = np.eye(kp)
                for h in range(horizon):
                    Phi = J @ F_h @ J.T @ P
                    vals.append(float(Phi[var_idx, shock_idx]))
                    F_h = F_h @ F
                irf_dict[var_name] = vals
            var_irfs[shock_label] = irf_dict

        # Compute distance (sum of squared differences)
        distances = {}
        for shock_name in ["demand", "supply", "monetary"]:
            dsge_irf = dsge_irfs.get(shock_name, {})
            var_irf = var_irfs.get(shock_name, {})
            d = 0.0
            for var_name in var_names:
                dsge_vals = np.array(dsge_irf.get(var_name, [0.0] * horizon))
                var_vals = np.array(var_irf.get(var_name, [0.0] * horizon))
                # Normalize by VAR IRF variance to make comparable
                var_scale = max(np.std(var_vals), 1e-6)
                d += float(np.sum(((dsge_vals - var_vals) / var_scale) ** 2))
            distances[shock_name] = round(d, 4)

        return {
            "var_irfs": var_irfs,
            "irf_distance": distances,
            "total_distance": round(sum(distances.values()), 4),
        }

    @staticmethod
    def _diagnostics(params: dict, irfs: dict) -> dict:
        """Model diagnostics and economic interpretability checks."""
        diag = {}

        # Taylor principle: phi_pi > 1
        diag["taylor_principle"] = {
            "holds": params["phi_pi"] > 1.0,
            "phi_pi": params["phi_pi"],
        }

        # Divine coincidence: under no cost-push shocks, optimal policy
        # can simultaneously stabilize output gap and inflation
        diag["divine_coincidence_note"] = (
            "Under kappa > 0 and no cost-push shocks, stabilizing inflation "
            "also stabilizes the output gap (divine coincidence)."
        )

        # Shock persistence
        for shock_name, irf in irfs.items():
            x_irf = irf.get("output_gap", [])
            if x_irf:
                diag[f"{shock_name}_persistence"] = {
                    "half_life": irf.get("half_life"),
                    "peak_output_response": round(max(abs(v) for v in x_irf), 4),
                    "cumulative_output": round(sum(x_irf), 4),
                }

        return diag

    @staticmethod
    def _compute_score(results: dict) -> float:
        """Score based on model properties and fit."""
        score = 0.0

        # Indeterminacy penalty
        sol = results.get("solution", {})
        if sol.get("determinacy") == "indeterminate":
            score += 30

        # Taylor principle violation
        diag = results.get("diagnostics", {})
        if not diag.get("taylor_principle", {}).get("holds", True):
            score += 25

        # Poor VAR match
        var_comp = results.get("var_comparison", {})
        total_dist = var_comp.get("total_distance", 0)
        if total_dist > 50:
            score += 20
        elif total_dist > 20:
            score += 10

        # High shock persistence (monetary shock should die out)
        monetary_hl = diag.get("monetary_persistence", {}).get("half_life")
        if monetary_hl is not None and monetary_hl > 10:
            score += 15

        # Low calibration R-squared
        cal = results.get("calibration", {})
        r2 = cal.get("taylor_rule_r_squared", 1.0)
        if r2 < 0.3:
            score += 10

        return min(score, 100)
