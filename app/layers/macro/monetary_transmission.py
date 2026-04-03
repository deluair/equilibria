"""Monetary Transmission - Interest rate, credit, and exchange rate channels. VAR impulse responses."""

from __future__ import annotations

import numpy as np
from scipy import linalg

from app.layers.base import LayerBase


class MonetaryTransmission(LayerBase):
    layer_id = "l2"
    name = "Monetary Transmission"
    weight = 0.05

    # Channel indicators (FRED)
    POLICY_RATE = "FEDFUNDS"

    INTEREST_RATE_CHANNEL = {
        "mortgage_rate": "MORTGAGE30US",
        "prime_rate": "DPRIME",
        "auto_rate": "TERMCBAUTO48NS",
        "aaa_yield": "AAA",
        "baa_yield": "BAA",
    }

    CREDIT_CHANNEL = {
        "bank_credit": "TOTBKCR",
        "ci_loans": "BUSLOANS",
        "consumer_credit": "TOTALSL",
        "lending_standards": "DRTSCILM",  # Senior Loan Officer Survey: tightening
    }

    EXCHANGE_RATE_CHANNEL = {
        "broad_dollar": "DTWEXBGS",
        "real_broad_dollar": "RTWEXBGS",
    }

    REAL_ECONOMY = {
        "ip": "INDPRO",          # Industrial Production
        "gdp": "GDP",
        "cpi": "CPIAUCSL",
        "unemployment": "UNRATE",
    }

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 15)
        var_lags = kwargs.get("var_lags", 4)

        all_channels = {
            self.POLICY_RATE: "policy_rate",
            **{v: k for k, v in self.INTEREST_RATE_CHANNEL.items()},
            **{v: k for k, v in self.CREDIT_CHANNEL.items()},
            **{v: k for k, v in self.EXCHANGE_RATE_CHANNEL.items()},
            **{v: k for k, v in self.REAL_ECONOMY.items()},
        }

        series_ids = list(all_channels.keys())
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

        results = {}

        # Interest rate pass-through: correlation of policy rate changes with market rates
        ir_passthrough = {}
        policy_data = series_map.get(self.POLICY_RATE, {})
        if policy_data:
            policy_dates = sorted(policy_data.keys())
            if len(policy_dates) >= 12:
                policy_changes = np.diff([policy_data[d] for d in policy_dates])
                for name, sid in self.INTEREST_RATE_CHANNEL.items():
                    channel_data = series_map.get(sid, {})
                    common = sorted(set(policy_dates) & set(channel_data.keys()))
                    if len(common) >= 12:
                        p_vals = np.array([policy_data[d] for d in common])
                        c_vals = np.array([channel_data[d] for d in common])
                        p_diff = np.diff(p_vals)
                        c_diff = np.diff(c_vals)

                        if np.std(p_diff) > 1e-12 and np.std(c_diff) > 1e-12:
                            corr = float(np.corrcoef(p_diff, c_diff)[0, 1])
                            # Pass-through coefficient: beta from regression of rate change on policy change
                            beta = float(np.sum(p_diff * c_diff) / np.sum(p_diff ** 2))
                            ir_passthrough[name] = {
                                "correlation": corr,
                                "passthrough_coefficient": beta,
                                "complete_passthrough": abs(beta - 1.0) < 0.2,
                            }

        results["interest_rate_channel"] = ir_passthrough

        # Credit channel: response of credit aggregates to policy rate
        credit_response = {}
        for name, sid in self.CREDIT_CHANNEL.items():
            channel_data = series_map.get(sid, {})
            common = sorted(set(policy_dates if policy_data else []) & set(channel_data.keys()))
            if len(common) >= 12:
                c_vals = np.array([channel_data[d] for d in common])
                # Growth rate
                c_growth = np.diff(c_vals) / c_vals[:-1] * 100
                p_vals = np.array([policy_data[d] for d in common[:-1]])

                if np.std(p_vals) > 1e-12 and np.std(c_growth) > 1e-12:
                    # Correlation of policy rate level with credit growth
                    corr = float(np.corrcoef(p_vals, c_growth)[0, 1])
                    credit_response[name] = {
                        "correlation_with_policy": corr,
                        "current_growth": float(c_growth[-1]) if len(c_growth) > 0 else None,
                        "mean_growth": float(np.mean(c_growth)),
                    }

        results["credit_channel"] = credit_response

        # Exchange rate channel
        fx_response = {}
        for name, sid in self.EXCHANGE_RATE_CHANNEL.items():
            channel_data = series_map.get(sid, {})
            common = sorted(set(policy_dates if policy_data else []) & set(channel_data.keys()))
            if len(common) >= 12:
                p_vals = np.array([policy_data[d] for d in common])
                f_vals = np.array([channel_data[d] for d in common])
                p_diff = np.diff(p_vals)
                f_diff = np.diff(f_vals)

                if np.std(p_diff) > 1e-12 and np.std(f_diff) > 1e-12:
                    corr = float(np.corrcoef(p_diff, f_diff)[0, 1])
                    fx_response[name] = {
                        "correlation": corr,
                        "current_level": float(f_vals[-1]),
                    }

        results["exchange_rate_channel"] = fx_response

        # Estimate reduced-form VAR for transmission analysis
        var_result = await self._estimate_var(
            db, series_map, country, var_lags
        )
        if var_result:
            results["var_analysis"] = var_result

        # Transmission speed: cumulative response at different horizons
        results["transmission_summary"] = self._summarize_transmission(
            ir_passthrough, credit_response, fx_response
        )

        # Score: tightness of monetary conditions
        score = self._compute_score(ir_passthrough, credit_response, fx_response, series_map)

        return {
            "score": score,
            "results": results,
        }

    async def _estimate_var(self, db, series_map: dict, country: str,
                            lags: int) -> dict | None:
        """Estimate a 3-variable VAR: policy rate, IP growth, CPI inflation."""
        policy = series_map.get(self.POLICY_RATE, {})
        ip = series_map.get("INDPRO", {})
        cpi = series_map.get("CPIAUCSL", {})

        common = sorted(set(policy.keys()) & set(ip.keys()) & set(cpi.keys()))
        if len(common) < lags + 24:
            return None

        # Build endogenous matrix: [policy_rate, ip_growth, cpi_inflation]
        p_arr = np.array([policy[d] for d in common])
        ip_arr = np.array([ip[d] for d in common])
        cpi_arr = np.array([cpi[d] for d in common])

        # Transform: levels for policy, log-diff for IP and CPI (annualized)
        ip_growth = np.diff(np.log(ip_arr)) * 1200  # annualized monthly growth
        cpi_infl = np.diff(np.log(cpi_arr)) * 1200
        p_level = p_arr[1:]

        n = len(ip_growth)
        if n < lags + 12:
            return None

        Y = np.column_stack([p_level, ip_growth, cpi_infl])
        var_names = ["policy_rate", "ip_growth", "cpi_inflation"]

        # OLS estimation of VAR(p)
        T = Y.shape[0]
        k = Y.shape[1]

        # Build lagged matrices
        Y_dep = Y[lags:]  # T-p x k
        X = np.ones((T - lags, 1))  # constant
        for lag in range(1, lags + 1):
            X = np.hstack([X, Y[lags - lag:T - lag]])

        # OLS: B = (X'X)^{-1} X'Y
        XtX_inv = np.linalg.inv(X.T @ X)
        B = XtX_inv @ X.T @ Y_dep  # (1 + k*p) x k

        # Residuals and covariance
        residuals = Y_dep - X @ B
        Sigma = (residuals.T @ residuals) / (T - lags - X.shape[1])

        # Cholesky IRF (ordering: policy, IP, CPI)
        P = np.linalg.cholesky(Sigma)

        # Companion form for IRF computation
        horizon = 24
        irfs = self._compute_irf(B, k, lags, P, horizon)

        # Format IRF results
        irf_formatted = {}
        for shock_idx, shock_name in enumerate(var_names):
            irf_formatted[shock_name] = {}
            for resp_idx, resp_name in enumerate(var_names):
                irf_formatted[shock_name][resp_name] = [
                    float(irfs[h][resp_idx, shock_idx]) for h in range(horizon)
                ]

        # Forecast error variance decomposition
        fevd = self._compute_fevd(irfs, horizon)
        fevd_formatted = {}
        for resp_idx, resp_name in enumerate(var_names):
            fevd_formatted[resp_name] = {
                var_names[shock_idx]: [
                    float(fevd[h][resp_idx, shock_idx]) for h in range(horizon)
                ]
                for shock_idx in range(k)
            }

        return {
            "var_lags": lags,
            "n_obs": T - lags,
            "variables": var_names,
            "residual_covariance": Sigma.tolist(),
            "irf": irf_formatted,
            "fevd": fevd_formatted,
        }

    @staticmethod
    def _compute_irf(B: np.ndarray, k: int, p: int,
                     P: np.ndarray, horizon: int) -> list[np.ndarray]:
        """Compute impulse response functions from VAR coefficients."""
        # Extract coefficient matrices A1, A2, ..., Ap (skip constant)
        A_mats = []
        for lag in range(p):
            A_mats.append(B[1 + lag * k:1 + (lag + 1) * k, :].T)

        # Companion matrix
        kp = k * p
        F = np.zeros((kp, kp))
        for lag in range(p):
            F[:k, lag * k:(lag + 1) * k] = A_mats[lag]
        if p > 1:
            F[k:, :k * (p - 1)] = np.eye(k * (p - 1))

        # IRF: Phi_h = J @ F^h @ J' @ P where J selects first k rows
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
        """Forecast error variance decomposition from IRFs."""
        k = irfs[0].shape[0]
        fevd = []

        cum_var = np.zeros((k, k))
        for h in range(horizon):
            cum_var += irfs[h] ** 2
            total_var = cum_var.sum(axis=1, keepdims=True)
            total_var[total_var < 1e-12] = 1.0
            fevd.append(cum_var / total_var)

        return fevd

    @staticmethod
    def _summarize_transmission(ir_pt: dict, credit: dict, fx: dict) -> dict:
        """Summarize transmission effectiveness across channels."""
        summary = {
            "interest_rate_effective": False,
            "credit_effective": False,
            "exchange_rate_effective": False,
        }

        # Interest rate channel: effective if avg pass-through > 0.5
        if ir_pt:
            avg_pt = np.mean([v["passthrough_coefficient"] for v in ir_pt.values()])
            summary["interest_rate_effective"] = float(avg_pt) > 0.5
            summary["avg_passthrough"] = float(avg_pt)

        # Credit channel: effective if negative correlation with policy
        if credit:
            corrs = [v["correlation_with_policy"] for v in credit.values()
                     if v.get("correlation_with_policy") is not None]
            if corrs:
                summary["credit_effective"] = float(np.mean(corrs)) < -0.2
                summary["avg_credit_correlation"] = float(np.mean(corrs))

        # Exchange rate channel: effective if positive correlation
        if fx:
            corrs = [v["correlation"] for v in fx.values()]
            if corrs:
                summary["exchange_rate_effective"] = float(np.mean(corrs)) > 0.2
                summary["avg_fx_correlation"] = float(np.mean(corrs))

        return summary

    def _compute_score(self, ir_pt: dict, credit: dict, fx: dict,
                       series_map: dict) -> float:
        """Score: high when monetary transmission is impaired or conditions are very tight."""
        scores = []

        # Pass-through impairment
        if ir_pt:
            avg_pt = np.mean([v["passthrough_coefficient"] for v in ir_pt.values()])
            # Low pass-through = impaired = higher stress
            scores.append(float(np.clip((1.0 - avg_pt) * 50, 0, 100)))

        # Lending standards tightening
        standards = series_map.get("DRTSCILM", {})
        if standards:
            latest = sorted(standards.keys())[-1]
            val = standards[latest]
            # Positive = net tightening. 40%+ tightening = very tight
            scores.append(float(np.clip(val * 1.5 + 20, 0, 100)))

        # Policy rate level relative to neutral (~2.5%)
        policy = series_map.get(self.POLICY_RATE, {})
        if policy:
            latest_rate = policy[sorted(policy.keys())[-1]]
            deviation = abs(latest_rate - 2.5)
            scores.append(float(np.clip(deviation * 12, 0, 100)))

        return float(np.clip(np.mean(scores) if scores else 50.0, 0, 100))
