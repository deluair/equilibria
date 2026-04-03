"""Financial development and economic growth nexus.

Estimates the effect of financial depth (domestic credit to private sector
as % of GDP) on economic growth, accounting for the non-linear "too much
finance" hypothesis where excessive financial development may harm growth.

Key references:
    King, R. & Levine, R. (1993). Finance and growth: Schumpeter might be
        right. QJE, 108(3), 717-737.
    Levine, R. (2005). Finance and growth: theory and evidence. Handbook of
        Economic Growth, 1, 865-934.
    Arcand, J., Berkes, E. & Panizza, U. (2015). Too much finance? Journal of
        Economic Growth, 20(2), 105-148.
"""

from __future__ import annotations

import numpy as np
import statsmodels.api as sm

from app.layers.base import LayerBase


class FinanceDevelopmentGrowth(LayerBase):
    layer_id = "l4"
    name = "Finance-Growth Nexus"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Estimate financial development effects on growth with non-linearity.

        Tests linear and quadratic specifications of credit/GDP on GDP
        growth. Estimates threshold above which finance may become harmful
        (Arcand et al. 2015 find ~100% of GDP).

        Returns dict with score, linear and quadratic coefficients,
        threshold estimate, and financial depth assessment.
        """
        country_iso3 = kwargs.get("country_iso3")

        # Fetch domestic credit to private sector (% of GDP)
        credit_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'FS.AST.PRVT.GD.ZS'
              AND dp.value IS NOT NULL
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Fetch GDP growth
        growth_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not credit_rows or not growth_rows:
            return {"score": 50, "results": {"error": "no financial depth or growth data"}}

        # Build panel
        credit_data: dict[str, dict[str, float]] = {}
        for r in credit_rows:
            credit_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        growth_data: dict[str, dict[str, float]] = {}
        for r in growth_rows:
            growth_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        y_list, x_list, x2_list = [], [], []
        obs_countries = []

        for iso in set(credit_data.keys()) & set(growth_data.keys()):
            common = sorted(set(credit_data[iso].keys()) & set(growth_data[iso].keys()))
            for yr in common:
                c = credit_data[iso][yr]
                g = growth_data[iso][yr]
                if c is not None and g is not None and c > 0:
                    y_list.append(g)
                    x_list.append(c)
                    x2_list.append(c ** 2)
                    obs_countries.append(iso)

        if len(y_list) < 30:
            return {"score": 50, "results": {"error": "insufficient finance-growth observations"}}

        y = np.array(y_list)
        credit = np.array(x_list)
        credit_sq = np.array(x2_list)

        # Model 1: Linear
        X1 = sm.add_constant(credit)
        m1 = sm.OLS(y, X1).fit(cov_type="HC1")

        # Model 2: Quadratic (too much finance)
        X2 = sm.add_constant(np.column_stack([credit, credit_sq]))
        m2 = sm.OLS(y, X2).fit(cov_type="HC1")

        beta1_linear = float(m1.params[1])
        beta1_quad = float(m2.params[1])
        beta2_quad = float(m2.params[2])

        # Threshold: turning point of quadratic
        threshold = None
        if beta2_quad != 0:
            tp = -beta1_quad / (2 * beta2_quad)
            if tp > 0:
                threshold = {
                    "credit_gdp_pct": float(tp),
                    "in_sample": float(np.min(credit)) <= tp <= float(np.max(credit)),
                    "too_much_finance": beta2_quad < 0 and float(m2.pvalues[2]) < 0.10,
                }

        # Model 3: Piecewise linear at Arcand threshold (100% of GDP)
        arcand_threshold = 100.0
        below = credit <= arcand_threshold
        above = credit > arcand_threshold
        credit_below = np.where(below, credit, arcand_threshold)
        credit_above = np.where(above, credit - arcand_threshold, 0)

        X3 = sm.add_constant(np.column_stack([credit_below, credit_above]))
        m3 = sm.OLS(y, X3).fit(cov_type="HC1")

        piecewise = {
            "below_100_coef": float(m3.params[1]),
            "below_100_pval": float(m3.pvalues[1]),
            "above_100_coef": float(m3.params[2]),
            "above_100_pval": float(m3.pvalues[2]),
            "n_below": int(np.sum(below)),
            "n_above": int(np.sum(above)),
        }

        # Target country assessment
        target_analysis = None
        if country_iso3 and country_iso3 in credit_data:
            latest_years = sorted(credit_data[country_iso3].keys())
            if latest_years:
                latest_credit = credit_data[country_iso3][latest_years[-1]]
                # Financial depth classification
                if latest_credit < 20:
                    depth_class = "shallow"
                elif latest_credit < 50:
                    depth_class = "moderate"
                elif latest_credit < 100:
                    depth_class = "deep"
                else:
                    depth_class = "very_deep"

                target_analysis = {
                    "credit_gdp_pct": latest_credit,
                    "depth_class": depth_class,
                    "year": latest_years[-1],
                    "above_arcand_threshold": latest_credit > arcand_threshold,
                }

        # Score: too shallow = stress, optimal range = stable, too deep = moderate concern
        if target_analysis:
            c = target_analysis["credit_gdp_pct"]
            if c < 15:
                score = 75  # Financial exclusion
            elif c < 30:
                score = 55  # Below optimal
            elif c < 80:
                score = 25  # Healthy range
            elif c < 120:
                score = 40  # Getting excessive
            else:
                score = 60  # Too much finance
        else:
            med_credit = float(np.median(credit))
            score = 50 - (min(med_credit, 80) - 50) * 0.5

        score = float(np.clip(score, 0, 100))

        results = {
            "linear": {
                "coef": beta1_linear,
                "se": float(m1.bse[1]),
                "pval": float(m1.pvalues[1]),
                "r_sq": float(m1.rsquared),
                "n_obs": int(m1.nobs),
            },
            "quadratic": {
                "beta_credit": beta1_quad,
                "beta_credit_sq": beta2_quad,
                "se_credit": float(m2.bse[1]),
                "se_credit_sq": float(m2.bse[2]),
                "pval_credit": float(m2.pvalues[1]),
                "pval_credit_sq": float(m2.pvalues[2]),
                "r_sq": float(m2.rsquared),
                "n_obs": int(m2.nobs),
                "threshold": threshold,
            },
            "piecewise": piecewise,
            "target": target_analysis,
            "summary_stats": {
                "mean_credit": float(np.mean(credit)),
                "median_credit": float(np.median(credit)),
                "std_credit": float(np.std(credit)),
                "min_credit": float(np.min(credit)),
                "max_credit": float(np.max(credit)),
            },
            "country_iso3": country_iso3,
        }

        return {"score": score, "results": results}
