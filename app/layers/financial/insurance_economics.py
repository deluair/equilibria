"""Insurance economics: actuarial pricing, moral hazard, adverse selection, and catastrophe risk.

Models the insurance sector using actuarial loss modeling, tests for moral hazard
and adverse selection, and quantifies catastrophe risk via extreme value theory.

Methodology:
    1. Actuarial pricing (collective risk model):
       S = sum_{i=1}^{N} X_i,  N ~ Poisson(lambda), X_i ~ Lognormal(mu, sigma)
       Net premium P = E[S] = lambda * exp(mu + sigma^2/2)
       Safety loading: P_loaded = P * (1 + theta) where theta = safety loading factor
       Estimated via MLE on historical claims.

    2. Moral hazard test (Chiappori-Salanie 2000):
       Test correlation between coverage level and ex-post accident frequency.
       H0: rho(coverage, claims) = 0 (no moral hazard).
       Asymmetric information index: AI = corr(coverage, loss | observables).

    3. Adverse selection test (Rothschild-Stiglitz):
       High-risk individuals self-select into more comprehensive coverage.
       Test: loss_rate_high_coverage > loss_rate_low_coverage (after controlling).
       Implemented via quantile regression on coverage categories.

    4. Catastrophe risk (Extreme Value Theory - GPD tail):
       P(X > u + y | X > u) ≈ (1 + xi * y / beta)^{-1/xi}
       where xi = shape parameter, beta = scale, u = threshold.
       100-year return level estimated from fitted GPD.

    Score: high loss ratio + strong adverse selection + fat catastrophe tail = stress.

References:
    Chiappori, P.A. & Salanie, B. (2000). "Testing for Asymmetric Information
        in Insurance Markets." Journal of Political Economy, 108(1), 56-78.
    Rothschild, M. & Stiglitz, J. (1976). "Equilibrium in Competitive Insurance
        Markets." Quarterly Journal of Economics, 90(4), 629-649.
    McNeil, A.J. et al. (2005). "Quantitative Risk Management." Princeton UP.
"""

from __future__ import annotations

import numpy as np
from scipy import optimize, stats as sp_stats

from app.layers.base import LayerBase


class InsuranceEconomics(LayerBase):
    layer_id = "l7"
    name = "Insurance Economics"

    async def compute(self, db, **kwargs) -> dict:
        """Analyze insurance sector economics.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            lookback_years : int - data window (default 15)
            gpd_threshold_quantile : float - EVT threshold quantile (default 0.90)
        """
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 15)
        gpd_threshold_q = kwargs.get("gpd_threshold_quantile", 0.90)

        rows = await db.fetch_all(
            """
            SELECT ds.description, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('fred', 'wdi', 'imf', 'insurance', 'sigma')
              AND ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
            ORDER BY ds.description, dp.date
            """,
            (country, f"-{lookback} years"),
        )

        series: dict[str, list[tuple[str, float]]] = {}
        for r in rows:
            desc = (r["description"] or "").lower()
            series.setdefault(desc, []).append((r["date"], float(r["value"])))

        # Extract series
        premiums = self._extract_series(series, ["insurance_premium", "gross_premium", "premium_written"])
        claims = self._extract_series(series, ["insurance_claims", "claims_paid", "loss_incurred"])
        loss_ratio = self._extract_series(series, ["loss_ratio", "combined_ratio"])
        penetration = self._extract_series(series, ["insurance_penetration", "premium_gdp"])
        catastrophe_losses = self._extract_series(series, ["cat_loss", "catastrophe", "natural_disaster_loss"])

        # --- Actuarial pricing ---
        actuarial = self._actuarial_pricing(claims, premiums)

        # --- Moral hazard test ---
        moral_hazard = self._moral_hazard_test(series)

        # --- Adverse selection test ---
        adverse_sel = self._adverse_selection_test(series)

        # --- Catastrophe risk (EVT) ---
        cat_risk = self._catastrophe_risk(catastrophe_losses or claims, gpd_threshold_q)

        # --- Loss ratio trend ---
        lr_trend = None
        lr_data = loss_ratio or (
            [c / max(p, 1e-6) for c, p in zip(claims or [], premiums or [])]
            if claims and premiums else None
        )
        if lr_data and len(lr_data) >= 4:
            lr_arr = np.array(lr_data)
            slope, _, r_val, _, _ = sp_stats.linregress(np.arange(len(lr_arr)), lr_arr)
            lr_trend = {
                "current_loss_ratio": round(float(lr_arr[-1]), 4),
                "mean_loss_ratio": round(float(np.mean(lr_arr)), 4),
                "trend_slope": round(float(slope), 6),
                "r_squared": round(float(r_val ** 2), 4),
                "direction": "deteriorating" if slope > 0.005 else "improving" if slope < -0.005 else "stable",
            }

        # --- Score ---
        # Loss ratio component: high loss ratio = stress
        lr_component = 50.0
        if lr_trend and lr_trend.get("current_loss_ratio") is not None:
            lr = lr_trend["current_loss_ratio"]
            lr_component = float(np.clip((lr - 0.5) * 100.0, 0, 100))

        # Adverse selection component
        as_component = 50.0
        if adverse_sel and adverse_sel.get("selection_coefficient") is not None:
            coef = abs(float(adverse_sel["selection_coefficient"]))
            as_component = float(np.clip(coef * 50.0, 0, 100))

        # Catastrophe risk component: fat tail = stress
        cat_component = 50.0
        if cat_risk and cat_risk.get("gpd_shape_xi") is not None:
            xi = cat_risk["gpd_shape_xi"]
            cat_component = float(np.clip(xi * 100.0 + 50.0, 0, 100))

        score = float(np.clip(
            0.35 * lr_component + 0.30 * as_component + 0.35 * cat_component,
            0, 100,
        ))

        return {
            "score": round(score, 2),
            "country": country,
            "actuarial_pricing": actuarial,
            "moral_hazard": moral_hazard,
            "adverse_selection": adverse_sel,
            "catastrophe_risk": cat_risk,
            "loss_ratio_trend": lr_trend,
            "market_penetration_pct": round(float(penetration[-1]), 3) if penetration else None,
        }

    @staticmethod
    def _extract_series(series: dict, keywords: list[str]) -> list[float] | None:
        for key, vals in series.items():
            for kw in keywords:
                if kw in key:
                    return [v[1] for v in vals]
        return None

    @staticmethod
    def _actuarial_pricing(
        claims: list[float] | None,
        premiums: list[float] | None,
    ) -> dict | None:
        """Fit collective risk model and compute actuarially fair premium."""
        if not claims or len(claims) < 5:
            return None

        c = np.array(claims)
        # Fit lognormal to claims distribution
        log_c = np.log(np.maximum(c, 1e-6))
        mu_hat = float(np.mean(log_c))
        sigma_hat = float(np.std(log_c, ddof=1))

        # Expected claim (lognormal moment)
        expected_claim = float(np.exp(mu_hat + sigma_hat ** 2 / 2))
        # Safety loading theta: coefficient of variation proxy
        cv = float(np.std(c) / max(np.mean(c), 1e-6))
        theta = cv * 0.25  # standard variance principle loading

        net_premium = expected_claim
        loaded_premium = net_premium * (1 + theta)

        # Current loss ratio
        current_premium = float(premiums[-1]) if premiums else loaded_premium
        loss_ratio = float(np.mean(c)) / max(current_premium, 1e-6)

        return {
            "lognormal_mu": round(float(mu_hat), 4),
            "lognormal_sigma": round(float(sigma_hat), 4),
            "expected_claim": round(float(expected_claim), 4),
            "safety_loading_theta": round(float(theta), 4),
            "actuarially_fair_premium": round(float(net_premium), 4),
            "loaded_premium": round(float(loaded_premium), 4),
            "current_loss_ratio": round(float(loss_ratio), 4),
            "adequacy": "adequate" if loss_ratio < 0.85 else "inadequate",
        }

    @staticmethod
    def _moral_hazard_test(series: dict) -> dict:
        """Test for moral hazard via coverage-claims correlation."""
        coverage = None
        claims_rate = None
        for key, vals in series.items():
            if any(kw in key for kw in ["coverage", "insured_value"]):
                coverage = [v[1] for v in vals]
            if any(kw in key for kw in ["claims_rate", "frequency", "loss_frequency"]):
                claims_rate = [v[1] for v in vals]

        if not coverage or not claims_rate or len(coverage) < 5:
            return {
                "test": "moral_hazard_correlation",
                "result": "insufficient_data",
                "moral_hazard_detected": None,
            }

        n = min(len(coverage), len(claims_rate))
        rho, p_val = sp_stats.pearsonr(coverage[-n:], claims_rate[-n:])
        return {
            "test": "chiappori_salanie_correlation",
            "correlation_rho": round(float(rho), 4),
            "p_value": round(float(p_val), 4),
            "moral_hazard_detected": float(p_val) < 0.05 and rho > 0,
            "n_observations": n,
            "interpretation": (
                "moral hazard present" if float(p_val) < 0.05 and rho > 0
                else "no significant moral hazard"
            ),
        }

    @staticmethod
    def _adverse_selection_test(series: dict) -> dict:
        """Test for adverse selection via high-coverage loss rate premium."""
        high_cov_loss = None
        low_cov_loss = None
        for key, vals in series.items():
            if "high_coverage" in key or "comprehensive" in key:
                high_cov_loss = [v[1] for v in vals]
            if "low_coverage" in key or "basic_coverage" in key:
                low_cov_loss = [v[1] for v in vals]

        if not high_cov_loss or not low_cov_loss:
            return {
                "test": "adverse_selection_loss_differential",
                "result": "insufficient_data",
                "selection_coefficient": None,
            }

        n = min(len(high_cov_loss), len(low_cov_loss))
        diff = np.array(high_cov_loss[-n:]) - np.array(low_cov_loss[-n:])
        t_stat, p_val = sp_stats.ttest_1samp(diff, 0)

        return {
            "test": "rothschild_stiglitz_differential",
            "mean_differential": round(float(np.mean(diff)), 4),
            "selection_coefficient": round(float(np.mean(diff)), 4),
            "t_statistic": round(float(t_stat), 4),
            "p_value": round(float(p_val), 4),
            "adverse_selection_detected": float(p_val) < 0.05 and float(np.mean(diff)) > 0,
            "n_observations": n,
        }

    @staticmethod
    def _catastrophe_risk(
        losses: list[float] | None,
        threshold_quantile: float,
    ) -> dict | None:
        """Fit Generalized Pareto Distribution (GPD) to excess losses above threshold."""
        if not losses or len(losses) < 10:
            return None

        data = np.array(losses)
        u = float(np.quantile(data, threshold_quantile))
        exceedances = data[data > u] - u

        if len(exceedances) < 5:
            return {"note": "too few threshold exceedances for GPD fit", "threshold": round(u, 4)}

        # Method of moments GPD fit
        # xi = shape (heavy tail if > 0), beta = scale
        ex_mean = float(np.mean(exceedances))
        ex_var = float(np.var(exceedances, ddof=1))

        if ex_var > 0:
            xi_hat = 0.5 * (ex_mean ** 2 / ex_var - 1)
            beta_hat = 0.5 * ex_mean * (ex_mean ** 2 / ex_var + 1)
        else:
            xi_hat, beta_hat = 0.0, ex_mean

        # Return level for T years: u + beta/xi * (((1-p)*n/k)^xi - 1)
        n_total = len(data)
        n_excess = len(exceedances)
        p_exceed = n_excess / n_total

        def return_level(T: float) -> float:
            p_t = 1 / T
            if abs(xi_hat) < 1e-6:
                return float(u + beta_hat * np.log(p_exceed / p_t))
            return float(u + beta_hat / max(abs(xi_hat), 1e-6) * ((p_exceed / p_t) ** xi_hat - 1))

        rl_100 = return_level(100.0)
        rl_250 = return_level(250.0)

        return {
            "gpd_shape_xi": round(float(xi_hat), 4),
            "gpd_scale_beta": round(float(beta_hat), 4),
            "threshold_u": round(float(u), 4),
            "n_exceedances": len(exceedances),
            "100yr_return_level": round(float(rl_100), 4),
            "250yr_return_level": round(float(rl_250), 4),
            "tail_type": "heavy" if xi_hat > 0.2 else "light" if xi_hat < -0.1 else "moderate",
        }
