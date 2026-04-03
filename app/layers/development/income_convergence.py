"""Income convergence: absolute beta-convergence test.

Tests whether poorer countries grow faster than richer ones (absolute
beta-convergence). A negative relationship between initial income and
subsequent growth indicates convergence; positive indicates divergence.

Key references:
    Barro, R. & Sala-i-Martin, X. (1992). Convergence. Journal of Political
        Economy, 100(2), 223-251.
    Mankiw, N., Romer, D. & Weil, D. (1992). A contribution to the empirics of
        economic growth. Quarterly Journal of Economics, 107(2), 407-437.
"""

from __future__ import annotations

import numpy as np
import statsmodels.api as sm

from app.layers.base import LayerBase


class IncomeConvergence(LayerBase):
    layer_id = "l4"
    name = "Income Convergence"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Absolute beta-convergence: initial income vs subsequent growth.

        Queries GDP per capita (constant USD) for levels and growth rates.
        Negative correlation implies convergence (lower stress); positive
        implies divergence (higher stress).

        Returns dict with score, beta coefficient, convergence hypothesis
        result, and target country relative position.
        """
        country_iso3 = kwargs.get("country_iso3")

        # GDP per capita, constant 2015 USD (levels)
        level_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # GDP per capita growth (annual %)
        growth_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD.ZG'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not level_rows or not growth_rows:
            return {"score": 50, "results": {"error": "insufficient income level or growth data"}}

        # Build country-level dicts
        level_data: dict[str, dict[str, float]] = {}
        for r in level_rows:
            level_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        growth_data: dict[str, dict[str, float]] = {}
        for r in growth_rows:
            growth_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        # For each country: initial income = first available level; avg growth = mean across period
        initial_income, avg_growth, sample_isos = [], [], []

        for iso in set(level_data.keys()) & set(growth_data.keys()):
            level_years = sorted(level_data[iso].keys())
            growth_years = sorted(growth_data[iso].keys())
            if not level_years or len(growth_years) < 5:
                continue
            init_income = level_data[iso][level_years[0]]
            mean_growth = np.mean([growth_data[iso][yr] for yr in growth_years])
            if init_income is not None and init_income > 0:
                initial_income.append(np.log(init_income))
                avg_growth.append(mean_growth)
                sample_isos.append(iso)

        if len(initial_income) < 20:
            return {"score": 50, "results": {"error": "insufficient countries for convergence test"}}

        y = np.array(avg_growth)
        X = sm.add_constant(np.array(initial_income))
        model = sm.OLS(y, X).fit(cov_type="HC1")

        beta_coef = float(model.params[1])
        beta_se = float(model.bse[1])
        beta_pval = float(model.pvalues[1])
        r_sq = float(model.rsquared)
        n_obs = int(model.nobs)

        convergence = beta_coef < 0 and beta_pval < 0.10
        divergence = beta_coef > 0 and beta_pval < 0.10

        # Target country analysis
        target_analysis = None
        if country_iso3 and country_iso3 in level_data and country_iso3 in growth_data:
            level_years = sorted(level_data[country_iso3].keys())
            growth_years = sorted(growth_data[country_iso3].keys())
            if level_years and growth_years:
                latest_level = level_data[country_iso3][level_years[-1]]
                mean_growth = np.mean([growth_data[country_iso3][yr] for yr in growth_years])
                all_latest = [
                    level_data[iso][sorted(level_data[iso].keys())[-1]]
                    for iso in level_data if level_data[iso]
                ]
                percentile = float(np.mean(np.array(all_latest) < latest_level) * 100)
                target_analysis = {
                    "latest_gdp_pc_kd": latest_level,
                    "income_percentile": percentile,
                    "avg_growth": mean_growth,
                    "below_median": percentile < 50,
                    "expected_to_converge": percentile < 50 and convergence,
                }

        # Score: divergence = high stress; convergence + country is poor = low stress
        if divergence:
            score = 75.0
        elif convergence:
            # Poor country catching up = low stress; rich country = moderate
            if target_analysis and target_analysis["below_median"]:
                score = 20.0
            else:
                score = 35.0
        else:
            # No significant relationship
            score = 50.0

        score = float(np.clip(score, 0, 100))

        return {
            "score": score,
            "results": {
                "beta_coef": beta_coef,
                "beta_se": beta_se,
                "beta_pval": beta_pval,
                "r_sq": r_sq,
                "n_obs": n_obs,
                "convergence": convergence,
                "divergence": divergence,
                "target": target_analysis,
                "country_iso3": country_iso3,
            },
        }
