"""Financial contagion analysis.

DCC-GARCH (Engle 2002) for time-varying dynamic conditional correlations.
Forbes-Rigobon (2002) contagion test correcting for heteroskedasticity bias
in crisis-period correlation increases. Regime-switching copula for joint
tail dependence estimation.

Score (0-100): based on correlation spike magnitude and contagion evidence.
Significant contagion or regime shift toward high-dependence pushes toward CRISIS.
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats
from scipy.optimize import minimize

from app.layers.base import LayerBase


class FinancialContagion(LayerBase):
    layer_id = "l7"
    name = "Financial Contagion"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        partner = kwargs.get("partner_iso3", "GBR")
        lookback = kwargs.get("lookback_years", 10)
        crisis_start = kwargs.get("crisis_start")  # date string
        crisis_end = kwargs.get("crisis_end")  # date string

        rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('fred', 'yahoo', 'asset_returns')
              AND ds.country_iso3 IN (?, ?)
              AND dp.date >= date('now', ?)
            ORDER BY dp.date
            """,
            (country, partner, f"-{lookback} years"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no return data"}

        # Split into two series
        series_a: dict[str, float] = {}
        series_b: dict[str, float] = {}
        for r in rows:
            if r["country_iso3"] == country:
                series_a[r["date"]] = float(r["value"])
            else:
                series_b[r["date"]] = float(r["value"])

        common_dates = sorted(set(series_a) & set(series_b))
        if len(common_dates) < 60:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient overlapping data"}

        ra = np.array([series_a[d] for d in common_dates])
        rb = np.array([series_b[d] for d in common_dates])
        n = len(ra)

        # DCC-GARCH: two-step estimation
        dcc_result = self._dcc_garch(ra, rb)

        # Forbes-Rigobon contagion test
        fr_result = None
        if crisis_start and crisis_end:
            # Split into calm and crisis periods
            calm_idx = []
            crisis_idx = []
            for i, d in enumerate(common_dates):
                if crisis_start <= d <= crisis_end:
                    crisis_idx.append(i)
                else:
                    calm_idx.append(i)

            if len(crisis_idx) >= 10 and len(calm_idx) >= 10:
                fr_result = self._forbes_rigobon_test(
                    ra[calm_idx], rb[calm_idx],
                    ra[crisis_idx], rb[crisis_idx],
                )

        # Regime-switching copula (simplified: two regimes via rolling window)
        copula_result = self._regime_copula(ra, rb, window=60)

        # Static correlation
        full_corr = float(np.corrcoef(ra, rb)[0, 1])

        # Rolling correlations
        rolling_corrs = self._rolling_correlation(ra, rb, window=60)

        # Tail dependence (empirical)
        tail_dep = self._empirical_tail_dependence(ra, rb)

        # Score: high dynamic correlation + contagion evidence = crisis
        max_dcc = float(np.max(dcc_result["correlations"])) if dcc_result else full_corr
        corr_component = float(np.clip(max_dcc * 60.0, 0, 100))

        contagion_component = 30.0
        if fr_result and fr_result["contagion_detected"]:
            contagion_component = 80.0

        tail_component = float(np.clip(tail_dep["lower"] * 200.0, 0, 100))

        score = float(np.clip(
            0.40 * corr_component + 0.35 * contagion_component + 0.25 * tail_component,
            0, 100,
        ))

        return {
            "score": round(score, 2),
            "country": country,
            "partner": partner,
            "n_obs": n,
            "static_correlation": round(full_corr, 4),
            "dcc_garch": {
                "dcc_a": round(dcc_result["a"], 6),
                "dcc_b": round(dcc_result["b"], 6),
                "mean_correlation": round(float(np.mean(dcc_result["correlations"])), 4),
                "max_correlation": round(float(np.max(dcc_result["correlations"])), 4),
                "min_correlation": round(float(np.min(dcc_result["correlations"])), 4),
                "current_correlation": round(float(dcc_result["correlations"][-1]), 4),
                "correlation_series": [
                    {"date": common_dates[i], "rho": round(float(dcc_result["correlations"][i]), 4)}
                    for i in range(0, n, max(n // 50, 1))
                ],
            } if dcc_result else None,
            "forbes_rigobon": {
                "rho_calm": round(fr_result["rho_calm"], 4),
                "rho_crisis": round(fr_result["rho_crisis"], 4),
                "rho_adjusted": round(fr_result["rho_adjusted"], 4),
                "test_statistic": round(fr_result["test_stat"], 4),
                "p_value": round(fr_result["p_value"], 4),
                "contagion_detected": fr_result["contagion_detected"],
            } if fr_result else None,
            "regime_copula": copula_result,
            "tail_dependence": {
                "lower": round(tail_dep["lower"], 4),
                "upper": round(tail_dep["upper"], 4),
            },
            "rolling_correlations": [
                {"date": common_dates[i + 59], "rho": round(float(rolling_corrs[i]), 4)}
                for i in range(0, len(rolling_corrs), max(len(rolling_corrs) // 50, 1))
            ] if rolling_corrs else None,
        }

    @staticmethod
    def _dcc_garch(ra: np.ndarray, rb: np.ndarray) -> dict | None:
        """DCC-GARCH (Engle 2002): two-step estimation.

        Step 1: Univariate GARCH(1,1) for each series.
        Step 2: DCC correlation dynamics on standardized residuals.
        """
        n = len(ra)
        if n < 30:
            return None

        # Step 1: GARCH(1,1) for each series (simplified)
        def fit_garch_11(eps):
            var = float(np.var(eps))
            omega = var * 0.05
            alpha = 0.08
            beta = 0.88
            sigma2 = np.zeros(n)
            sigma2[0] = var
            for t in range(1, n):
                sigma2[t] = omega + alpha * eps[t - 1] ** 2 + beta * sigma2[t - 1]
                sigma2[t] = max(sigma2[t], 1e-10)
            return np.sqrt(sigma2)

        sigma_a = fit_garch_11(ra - np.mean(ra))
        sigma_b = fit_garch_11(rb - np.mean(rb))

        # Standardized residuals
        z_a = (ra - np.mean(ra)) / np.maximum(sigma_a, 1e-10)
        z_b = (rb - np.mean(rb)) / np.maximum(sigma_b, 1e-10)

        # Step 2: DCC dynamics
        # Q(t) = (1-a-b)*Qbar + a*z(t-1)z(t-1)' + b*Q(t-1)
        # R(t) = diag(Q(t))^{-1/2} * Q(t) * diag(Q(t))^{-1/2}
        qbar = float(np.mean(z_a * z_b))

        def dcc_neg_ll(params):
            a, b = params
            if a < 0 or b < 0 or a + b >= 1:
                return 1e10
            q = np.zeros(n)
            q[0] = qbar
            for t in range(1, n):
                q[t] = (1 - a - b) * qbar + a * z_a[t - 1] * z_b[t - 1] + b * q[t - 1]
            # Correlation
            rho = np.clip(q, -0.999, 0.999)
            ll = -0.5 * np.sum(np.log(1 - rho ** 2) +
                               (z_a ** 2 + z_b ** 2 - 2 * rho * z_a * z_b) / (1 - rho ** 2))
            return -ll + 0.5 * np.sum(z_a ** 2 + z_b ** 2)

        result = minimize(dcc_neg_ll, [0.02, 0.95], method="L-BFGS-B",
                          bounds=[(1e-6, 0.3), (0.5, 0.999)])

        a, b = result.x
        q = np.zeros(n)
        q[0] = qbar
        for t in range(1, n):
            q[t] = (1 - a - b) * qbar + a * z_a[t - 1] * z_b[t - 1] + b * q[t - 1]

        correlations = np.clip(q, -0.999, 0.999)

        return {"a": float(a), "b": float(b), "correlations": correlations}

    @staticmethod
    def _forbes_rigobon_test(ra_calm: np.ndarray, rb_calm: np.ndarray,
                              ra_crisis: np.ndarray, rb_crisis: np.ndarray) -> dict:
        """Forbes-Rigobon (2002) adjusted correlation test for contagion.

        The naive crisis-period correlation is biased upward due to increased
        variance. FR adjust: rho_adj = rho_c / sqrt(1 + delta * (1 - rho_c^2))
        where delta = (var_crisis - var_calm) / var_calm.
        """
        rho_calm = float(np.corrcoef(ra_calm, rb_calm)[0, 1])
        rho_crisis = float(np.corrcoef(ra_crisis, rb_crisis)[0, 1])

        var_calm = float(np.var(ra_calm))
        var_crisis = float(np.var(ra_crisis))

        delta = (var_crisis - var_calm) / max(var_calm, 1e-10)

        # Adjusted correlation
        denom = 1 + delta * (1 - rho_crisis ** 2)
        rho_adjusted = rho_crisis / np.sqrt(max(denom, 1e-10))

        # Fisher z-transform test
        n_calm = len(ra_calm)
        n_crisis = len(ra_crisis)

        z_calm = np.arctanh(np.clip(rho_calm, -0.999, 0.999))
        z_adj = np.arctanh(np.clip(rho_adjusted, -0.999, 0.999))

        se = np.sqrt(1 / max(n_calm - 3, 1) + 1 / max(n_crisis - 3, 1))
        test_stat = (z_adj - z_calm) / max(se, 1e-10)
        p_value = float(2 * (1 - sp_stats.norm.cdf(abs(test_stat))))

        return {
            "rho_calm": rho_calm,
            "rho_crisis": rho_crisis,
            "rho_adjusted": float(rho_adjusted),
            "delta": float(delta),
            "test_stat": float(test_stat),
            "p_value": p_value,
            "contagion_detected": p_value < 0.05 and rho_adjusted > rho_calm,
        }

    @staticmethod
    def _regime_copula(ra: np.ndarray, rb: np.ndarray, window: int) -> dict:
        """Simplified regime detection via rolling Kendall's tau.

        Identifies high-dependence and low-dependence regimes.
        """
        n = len(ra)
        if n < window + 10:
            return {"n_regimes": 1, "current_regime": "unknown"}

        taus = []
        for i in range(window, n):
            tau, _ = sp_stats.kendalltau(ra[i - window:i], rb[i - window:i])
            taus.append(tau)

        taus = np.array(taus)
        median_tau = float(np.median(taus))

        # Two regimes: above/below median
        high_dep = taus >= median_tau
        current = "high_dependence" if high_dep[-1] else "low_dependence"

        return {
            "n_regimes": 2,
            "current_regime": current,
            "median_kendall_tau": round(median_tau, 4),
            "current_kendall_tau": round(float(taus[-1]), 4),
            "fraction_high_regime": round(float(np.mean(high_dep)), 4),
        }

    @staticmethod
    def _empirical_tail_dependence(ra: np.ndarray, rb: np.ndarray,
                                    quantile: float = 0.05) -> dict:
        """Empirical tail dependence coefficients.

        Lower: P(U <= q | V <= q) where U, V are empirical CDFs.
        Upper: P(U >= 1-q | V >= 1-q).
        """
        n = len(ra)
        # Rank-based pseudo-observations
        ranks_a = sp_stats.rankdata(ra) / (n + 1)
        ranks_b = sp_stats.rankdata(rb) / (n + 1)

        # Lower tail
        lower_mask = (ranks_a <= quantile) & (ranks_b <= quantile)
        lower_count = int(np.sum(lower_mask))
        lower_expected = int(np.sum(ranks_a <= quantile))
        lower_dep = lower_count / max(lower_expected, 1)

        # Upper tail
        upper_mask = (ranks_a >= 1 - quantile) & (ranks_b >= 1 - quantile)
        upper_count = int(np.sum(upper_mask))
        upper_expected = int(np.sum(ranks_a >= 1 - quantile))
        upper_dep = upper_count / max(upper_expected, 1)

        return {"lower": float(lower_dep), "upper": float(upper_dep)}

    @staticmethod
    def _rolling_correlation(ra: np.ndarray, rb: np.ndarray,
                             window: int) -> list[float]:
        """Rolling Pearson correlation."""
        n = len(ra)
        if n < window:
            return []
        corrs = []
        for i in range(window, n + 1):
            c = np.corrcoef(ra[i - window:i], rb[i - window:i])[0, 1]
            corrs.append(float(c))
        return corrs
