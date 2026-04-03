"""Sigma convergence: cross-country income dispersion over time.

Tests whether the cross-sectional standard deviation of log GDP per capita
is declining over time. A declining sigma indicates convergence in income
levels across countries (reducing inequality between nations).

Key references:
    Barro, R. & Sala-i-Martin, X. (1992). Convergence. JPE, 100(2), 223-251.
    Young, A., Higgins, M. & Levy, D. (2008). Sigma convergence versus beta
        convergence. Economics Letters, 99(3), 491-495.
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats
import statsmodels.api as sm

from app.layers.base import LayerBase


class SigmaConvergence(LayerBase):
    layer_id = "l4"
    name = "Sigma Convergence"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Compute cross-country dispersion of log GDP per capita over time.

        For each year, calculates the standard deviation and coefficient of
        variation of log GDP per capita across countries. Tests for a
        significant time trend in dispersion.

        Returns dict with score, dispersion time series, trend coefficient,
        and test for declining dispersion.
        """
        rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
              AND dp.value > 0
            ORDER BY dp.date, ds.country_iso3
            """
        )

        if not rows:
            return {"score": 50, "results": {"error": "no GDP per capita data"}}

        # Group by year
        by_year: dict[str, list[float]] = {}
        for r in rows:
            by_year.setdefault(r["date"][:4], []).append(r["value"])

        # Require at least 20 countries per year for valid dispersion
        min_countries = 20
        years_sorted = sorted(y for y, vals in by_year.items() if len(vals) >= min_countries)

        if len(years_sorted) < 5:
            return {"score": 50, "results": {"error": "insufficient years with enough countries"}}

        sigma_series = []
        cv_series = []
        mean_series = []
        n_countries_series = []

        for yr in years_sorted:
            vals = np.array([v for v in by_year[yr] if v > 0])
            log_vals = np.log(vals)
            sigma_series.append(float(np.std(log_vals, ddof=1)))
            cv_series.append(float(np.std(log_vals, ddof=1) / np.mean(log_vals)))
            mean_series.append(float(np.mean(log_vals)))
            n_countries_series.append(len(vals))

        sigma = np.array(sigma_series)
        years_num = np.arange(len(sigma))

        # Test for time trend in sigma: sigma_t = a + b*t + e
        X = sm.add_constant(years_num)
        model = sm.OLS(sigma, X)
        result = model.fit(cov_type="HC1")
        trend_coef = float(result.params[1])
        trend_se = float(result.bse[1])
        trend_pval = float(result.pvalues[1])

        # Overall change
        sigma_initial = sigma[0]
        sigma_final = sigma[-1]
        pct_change = (sigma_final - sigma_initial) / sigma_initial * 100

        # Structural break test: check if trend changed direction
        # Split at midpoint and test both halves
        mid = len(sigma) // 2
        structural_break = None
        if mid >= 3 and len(sigma) - mid >= 3:
            X1 = sm.add_constant(np.arange(mid))
            r1 = sm.OLS(sigma[:mid], X1).fit()
            X2 = sm.add_constant(np.arange(len(sigma) - mid))
            r2 = sm.OLS(sigma[mid:], X2).fit()
            if np.sign(r1.params[1]) != np.sign(r2.params[1]):
                structural_break = {
                    "break_year": years_sorted[mid],
                    "trend_before": float(r1.params[1]),
                    "trend_after": float(r2.params[1]),
                }

        # Score: declining sigma = convergence = low score (stable)
        # Rising sigma = divergence = high score (stress)
        if trend_coef < 0 and trend_pval < 0.05:
            score = max(10, 30 - abs(trend_coef) * 1000)
        elif trend_coef < 0:
            score = 40
        elif trend_pval < 0.05:
            score = min(90, 60 + trend_coef * 1000)
        else:
            score = 50

        score = float(np.clip(score, 0, 100))

        results = {
            "years": years_sorted,
            "sigma": sigma_series,
            "cv": cv_series,
            "mean_log_gdp": mean_series,
            "n_countries": n_countries_series,
            "trend": {
                "coef": trend_coef,
                "se": trend_se,
                "pval": trend_pval,
                "r_sq": float(result.rsquared),
            },
            "sigma_initial": sigma_initial,
            "sigma_final": sigma_final,
            "pct_change": pct_change,
            "converging": trend_coef < 0 and trend_pval < 0.05,
            "structural_break": structural_break,
        }

        return {"score": score, "results": results}
