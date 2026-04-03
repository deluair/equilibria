"""Fintech disruption analysis: digital lending, bank disintermediation, and financial inclusion.

Models the impact of fintech on traditional banking through digital lending growth,
bank disintermediation, unbanked population reduction, and regulatory sandbox effects.

Methodology:
    1. Digital lending growth (logistic diffusion):
       S_t = S_max / (1 + exp(-r*(t - t_0)))
       where S_max = market saturation, r = growth rate, t_0 = inflection point.
       Estimated via nonlinear least squares on credit outstanding.

    2. Bank disintermediation index:
       DI = 1 - (bank_credit / total_credit)
       Trend in DI captures migration of credit to non-bank channels.
       Structural break test (Chow) identifies regime change.

    3. Financial inclusion (unbanked reduction):
       Unbanked_t = Unbanked_0 * exp(-lambda * t) + phi * mobile_penetration_t
       where lambda = natural decay, phi = mobile adoption coefficient.

    4. Regulatory sandbox effects:
       Difference-in-differences around sandbox launch:
       Y_it = alpha_i + delta_t + beta * Post_t * Treat_i + e_it
       Identifies causal effect of regulatory liberalization.

    Score: rapid disintermediation + regulatory gap + low inclusion = high stress.

References:
    FSB (2022). "FinTech and Market Structure in Financial Services."
    Claessens, S. et al. (2018). "Fintech Credit: Market Structure, Business
        Models and Financial Stability Implications." BIS Working Paper 651.
    Demirguc-Kunt, A. et al. (2022). "The Global Findex Database 2021."
        World Bank.
"""

from __future__ import annotations

import numpy as np
from scipy import optimize
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class FintechDisruption(LayerBase):
    layer_id = "l7"
    name = "Fintech Disruption"

    async def compute(self, db, **kwargs) -> dict:
        """Measure fintech disruption of banking sector.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            lookback_years : int - data window (default 10)
            sandbox_year : int - year sandbox launched (optional)
        """
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 10)
        sandbox_year = kwargs.get("sandbox_year")

        rows = await db.fetch_all(
            """
            SELECT ds.description, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('fred', 'wdi', 'imf', 'bis', 'fintech')
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
        digital_lending = self._extract_series(series, ["fintech_credit", "digital_lending", "p2p_credit"])
        bank_credit = self._extract_series(series, ["bank_credit", "domestic_credit", "private_credit_bank"])
        total_credit = self._extract_series(series, ["total_credit", "private_credit", "credit_to_gdp"])
        mobile_money = self._extract_series(series, ["mobile_money", "mobile_payments", "digital_payments"])
        unbanked = self._extract_series(series, ["unbanked", "account_ownership", "financial_access"])
        mobile_penetration = self._extract_series(series, ["mobile_penetration", "mobile_subscriptions"])

        # --- Digital lending growth (logistic) ---
        lending_growth = None
        if digital_lending and len(digital_lending) >= 5:
            lending_growth = self._logistic_fit(np.array(digital_lending))

        # --- Disintermediation index ---
        disintermed = None
        if bank_credit and total_credit and len(bank_credit) >= 4:
            disintermed = self._disintermediation(
                np.array(bank_credit[-min(len(bank_credit), len(total_credit)):]),
                np.array(total_credit[-min(len(bank_credit), len(total_credit)):]),
            )

        # --- Unbanked reduction model ---
        inclusion = None
        if unbanked and len(unbanked) >= 3:
            inclusion = self._financial_inclusion(
                np.array(unbanked),
                np.array(mobile_penetration) if mobile_penetration else None,
            )

        # --- Regulatory sandbox effect ---
        sandbox = None
        if sandbox_year and (digital_lending or mobile_money):
            target = digital_lending or mobile_money
            sandbox = self._sandbox_effect(np.array(target), sandbox_year, lookback)

        # --- Score ---
        # High disintermediation pace = stress (rapid change without regulatory catch-up)
        di_component = 50.0
        if disintermed and disintermed.get("di_trend_slope") is not None:
            slope = disintermed["di_trend_slope"]
            di_component = float(np.clip(slope * 500.0 + 50.0, 0, 100))

        # Low financial inclusion = stress
        incl_component = 50.0
        if inclusion and inclusion.get("current_unbanked_pct") is not None:
            unb = inclusion["current_unbanked_pct"]
            incl_component = float(np.clip(unb, 0, 100))

        # Rapid lending growth without sandbox = stress; with sandbox = moderate
        growth_component = 50.0
        if lending_growth and lending_growth.get("annual_growth_pct") is not None:
            g = lending_growth["annual_growth_pct"]
            sandbox_bonus = -10.0 if sandbox and sandbox.get("effect_significant") else 10.0
            growth_component = float(np.clip(g * 2.0 + sandbox_bonus, 0, 100))

        score = float(np.clip(
            0.35 * di_component + 0.35 * incl_component + 0.30 * growth_component,
            0, 100,
        ))

        return {
            "score": round(score, 2),
            "country": country,
            "digital_lending_growth": lending_growth,
            "disintermediation": disintermed,
            "financial_inclusion": inclusion,
            "regulatory_sandbox": sandbox,
        }

    @staticmethod
    def _extract_series(series: dict, keywords: list[str]) -> list[float] | None:
        for key, vals in series.items():
            for kw in keywords:
                if kw in key:
                    return [v[1] for v in vals]
        return None

    @staticmethod
    def _logistic_fit(data: np.ndarray) -> dict:
        """Fit logistic growth to digital lending data."""
        n = len(data)
        t = np.arange(n, dtype=float)
        # Normalize
        max_val = float(data.max())
        norm = np.clip(data / max(max_val, 1e-6), 0.001, 0.999)

        def logistic(t_arr, r, t0):
            return 1.0 / (1.0 + np.exp(-r * (t_arr - t0)))

        try:
            popt, _ = optimize.curve_fit(
                logistic, t, norm,
                p0=[0.5, n / 2.0],
                bounds=([0.01, 0], [5.0, float(n) * 2]),
                maxfev=3000,
            )
            r, t0 = popt
            fitted = logistic(t, r, t0) * max_val
            ss_res = float(np.sum((data - fitted) ** 2))
            ss_tot = float(np.sum((data - data.mean()) ** 2))
            r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
            annual_growth_pct = float((data[-1] - data[0]) / max(data[0], 1e-6) / n * 100.0)
            return {
                "growth_rate_r": round(float(r), 4),
                "inflection_period": round(float(t0), 1),
                "r_squared": round(float(r2), 4),
                "annual_growth_pct": round(annual_growth_pct, 2),
                "saturation_level": round(float(max_val), 3),
            }
        except (RuntimeError, ValueError):
            slope, _, r_val, _, _ = sp_stats.linregress(t, data)
            annual_growth_pct = float(slope / max(data.mean(), 1e-6) * 100.0)
            return {
                "annual_growth_pct": round(annual_growth_pct, 2),
                "r_squared": round(float(r_val ** 2), 4),
                "note": "logistic fit failed; linear trend used",
            }

    @staticmethod
    def _disintermediation(bank_credit: np.ndarray, total_credit: np.ndarray) -> dict:
        """Compute bank disintermediation index and trend."""
        n = len(bank_credit)
        di = 1.0 - bank_credit / np.maximum(total_credit, 1e-6)
        slope, intercept, r_val, _, se = sp_stats.linregress(np.arange(n), di)

        # Chow structural break (split at midpoint)
        mid = n // 2
        if mid >= 3 and (n - mid) >= 3:
            slope1, *_ = sp_stats.linregress(np.arange(mid), di[:mid])
            slope2, *_ = sp_stats.linregress(np.arange(n - mid), di[mid:])
            break_detected = abs(slope2 - slope1) > abs(slope) * 2.0
        else:
            slope1 = slope2 = None
            break_detected = False

        return {
            "current_di": round(float(di[-1]), 4),
            "di_trend_slope": round(float(slope), 6),
            "r_squared": round(float(r_val ** 2), 4),
            "structural_break": {
                "detected": break_detected,
                "slope_early": round(float(slope1), 6) if slope1 is not None else None,
                "slope_late": round(float(slope2), 6) if slope2 is not None else None,
            },
            "direction": "accelerating" if slope > 1e-3 else "stable",
        }

    @staticmethod
    def _financial_inclusion(
        unbanked: np.ndarray,
        mobile: np.ndarray | None,
    ) -> dict:
        """Model unbanked population reduction driven by mobile adoption."""
        n = len(unbanked)
        t = np.arange(n, dtype=float)

        # Exponential decay of unbanked
        log_u = np.log(np.maximum(unbanked, 0.1))
        slope, intercept, r_val, _, _ = sp_stats.linregress(t, log_u)
        decay_lambda = -slope  # positive = reduction

        # Mobile adoption contribution
        mobile_coef = None
        if mobile is not None and len(mobile) >= n:
            mob = np.array(mobile[-n:])
            X = np.column_stack([np.ones(n), mob])
            beta = np.linalg.lstsq(X, unbanked, rcond=None)[0]
            mobile_coef = float(beta[1])  # negative = mobile reduces unbanked

        # Forecast: 5 years
        t_future = n + 5.0
        forecast = float(np.exp(intercept + slope * t_future))

        return {
            "current_unbanked_pct": round(float(unbanked[-1]), 2),
            "decay_lambda_yr": round(float(decay_lambda), 4),
            "mobile_adoption_coef": round(float(mobile_coef), 4) if mobile_coef is not None else None,
            "r_squared": round(float(r_val ** 2), 4),
            "forecast_5yr_unbanked_pct": round(float(np.clip(forecast, 0, 100)), 2),
            "inclusion_trajectory": "improving" if decay_lambda > 0 else "worsening",
        }

    @staticmethod
    def _sandbox_effect(data: np.ndarray, sandbox_year: int, lookback: int) -> dict:
        """Estimate regulatory sandbox causal effect via before/after comparison."""
        n = len(data)
        # Approximate index of sandbox launch
        implied_start_yr = 2024 - lookback
        launch_idx = sandbox_year - implied_start_yr
        launch_idx = max(2, min(n - 2, int(launch_idx)))

        pre = data[:launch_idx]
        post = data[launch_idx:]
        if len(pre) < 2 or len(post) < 2:
            return {"effect_significant": False, "note": "insufficient data around launch"}

        # Mean growth rate pre vs post
        growth_pre = float(np.mean(np.diff(pre) / np.maximum(pre[:-1], 1e-6))) if len(pre) > 1 else 0.0
        growth_post = float(np.mean(np.diff(post) / np.maximum(post[:-1], 1e-6))) if len(post) > 1 else 0.0

        # Two-sample t-test on growth rates
        if len(pre) > 2 and len(post) > 2:
            pre_rates = np.diff(pre) / np.maximum(pre[:-1], 1e-6)
            post_rates = np.diff(post) / np.maximum(post[:-1], 1e-6)
            t_stat, p_val = sp_stats.ttest_ind(pre_rates, post_rates)
        else:
            t_stat, p_val = 0.0, 1.0

        return {
            "sandbox_year": sandbox_year,
            "growth_pre_launch": round(float(growth_pre * 100), 2),
            "growth_post_launch": round(float(growth_post * 100), 2),
            "growth_acceleration_pct": round(float((growth_post - growth_pre) * 100), 2),
            "t_statistic": round(float(t_stat), 4),
            "p_value": round(float(p_val), 4),
            "effect_significant": float(p_val) < 0.10 and growth_post > growth_pre,
        }
