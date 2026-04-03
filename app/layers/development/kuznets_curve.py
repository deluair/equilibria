"""Kuznets curve: inverted-U relationship between income and inequality.

Tests the hypothesis that inequality first rises then falls as economies
develop. Estimates the turning point using panel data with country fixed
effects and a quadratic specification in log GDP per capita.

Key references:
    Kuznets, S. (1955). Economic growth and income inequality. AER, 45(1), 1-28.
    Ahluwalia, M. (1976). Inequality, poverty and development. Journal of
        Development Economics, 3(4), 307-342.
    Milanovic, B. (2016). Global Inequality: A New Approach for the Age of
        Globalization. Harvard University Press.
"""

from __future__ import annotations

import numpy as np
import statsmodels.api as sm

from app.layers.base import LayerBase


class KuznetsCurve(LayerBase):
    layer_id = "l4"
    name = "Kuznets Curve"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Estimate inverted-U relationship between income and inequality.

        Fetches Gini coefficient and GDP per capita data. Estimates
        quadratic specification with country fixed effects and tests
        for the existence and location of a turning point.

        Returns dict with score, quadratic coefficients, turning point
        estimate, and whether data supports the Kuznets hypothesis.
        """
        country_iso3 = kwargs.get("country_iso3")

        # Fetch Gini coefficient
        gini_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SI.POV.GINI'
              AND dp.value > 0 AND dp.value <= 100
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Fetch GDP per capita
        gdp_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
              AND dp.value > 0
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not gini_rows or not gdp_rows:
            return {"score": 50, "results": {"error": "no Gini or GDP data"}}

        # Build matched panel
        gini_data: dict[str, dict[str, float]] = {}
        for r in gini_rows:
            gini_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        gdp_data: dict[str, dict[str, float]] = {}
        for r in gdp_rows:
            gdp_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        # Build estimation sample
        y_list, x_list, x2_list, country_ids = [], [], [], []
        country_map: dict[str, int] = {}
        counter = 0

        for iso in set(gini_data.keys()) & set(gdp_data.keys()):
            common = sorted(set(gini_data[iso].keys()) & set(gdp_data[iso].keys()))
            for yr in common:
                gini_val = gini_data[iso][yr]
                gdp_val = gdp_data[iso][yr]
                if gini_val > 0 and gdp_val > 0:
                    log_gdp = np.log(gdp_val)
                    y_list.append(gini_val)
                    x_list.append(log_gdp)
                    x2_list.append(log_gdp ** 2)
                    if iso not in country_map:
                        country_map[iso] = counter
                        counter += 1
                    country_ids.append(country_map[iso])

        if len(y_list) < 30:
            return {"score": 50, "results": {"error": "insufficient Gini-GDP pairs"}}

        y = np.array(y_list)
        log_gdp = np.array(x_list)
        log_gdp_sq = np.array(x2_list)

        # Model 1: Pooled OLS (quadratic)
        X_pooled = sm.add_constant(np.column_stack([log_gdp, log_gdp_sq]))
        m_pooled = sm.OLS(y, X_pooled).fit(cov_type="HC1")

        beta1_pooled = float(m_pooled.params[1])
        beta2_pooled = float(m_pooled.params[2])

        # Model 2: Fixed effects (country dummies)
        fe_dummies = np.zeros((len(y), len(country_map) - 1))
        for i, cid in enumerate(country_ids):
            if cid > 0:
                fe_dummies[i, cid - 1] = 1

        X_fe = np.column_stack([log_gdp, log_gdp_sq, fe_dummies])
        X_fe = sm.add_constant(X_fe, has_constant="add")
        m_fe = sm.OLS(y, X_fe).fit(cov_type="HC1")

        beta1_fe = float(m_fe.params[1])
        beta2_fe = float(m_fe.params[2])

        # Turning point: -beta1 / (2 * beta2)
        turning_point_pooled = None
        if beta2_pooled != 0:
            tp_log = -beta1_pooled / (2 * beta2_pooled)
            turning_point_pooled = {
                "log_gdp": tp_log,
                "gdp_level": float(np.exp(tp_log)),
                "in_sample": float(np.min(log_gdp)) <= tp_log <= float(np.max(log_gdp)),
            }

        turning_point_fe = None
        if beta2_fe != 0:
            tp_log = -beta1_fe / (2 * beta2_fe)
            turning_point_fe = {
                "log_gdp": tp_log,
                "gdp_level": float(np.exp(tp_log)),
                "in_sample": float(np.min(log_gdp)) <= tp_log <= float(np.max(log_gdp)),
            }

        # Test for inverted-U: need beta1 > 0 and beta2 < 0
        inverted_u_pooled = beta1_pooled > 0 and beta2_pooled < 0
        inverted_u_fe = beta1_fe > 0 and beta2_fe < 0

        # Joint significance test for quadratic term
        f_test_pval = float(m_fe.pvalues[2]) if len(m_fe.pvalues) > 2 else 1.0

        # Target country: where on the curve?
        target_analysis = None
        if country_iso3 and country_iso3 in gini_data and country_iso3 in gdp_data:
            latest_gini_years = sorted(gini_data[country_iso3].keys())
            latest_gdp_years = sorted(gdp_data[country_iso3].keys())
            if latest_gini_years and latest_gdp_years:
                latest_gini = gini_data[country_iso3][latest_gini_years[-1]]
                latest_gdp = gdp_data[country_iso3][latest_gdp_years[-1]]
                log_gdp_target = np.log(latest_gdp)

                # Position relative to turning point
                if turning_point_fe:
                    tp = turning_point_fe["log_gdp"]
                    if log_gdp_target < tp:
                        phase = "rising" if inverted_u_fe else "indeterminate"
                    else:
                        phase = "falling" if inverted_u_fe else "indeterminate"
                else:
                    phase = "no_turning_point"

                target_analysis = {
                    "gini": latest_gini,
                    "gdp_per_capita": latest_gdp,
                    "log_gdp": float(log_gdp_target),
                    "phase": phase,
                }

        # Score: high inequality = high score (stress)
        if target_analysis:
            gini = target_analysis["gini"]
            if gini < 30:
                score = 20
            elif gini < 35:
                score = 35
            elif gini < 40:
                score = 50
            elif gini < 50:
                score = 65
            else:
                score = 80
        else:
            avg_gini = float(np.mean(y))
            score = avg_gini  # Gini maps naturally to 0-100

        score = float(np.clip(score, 0, 100))

        results = {
            "pooled_ols": {
                "beta_log_gdp": beta1_pooled,
                "beta_log_gdp_sq": beta2_pooled,
                "se_log_gdp": float(m_pooled.bse[1]),
                "se_log_gdp_sq": float(m_pooled.bse[2]),
                "pval_log_gdp": float(m_pooled.pvalues[1]),
                "pval_log_gdp_sq": float(m_pooled.pvalues[2]),
                "r_sq": float(m_pooled.rsquared),
                "n_obs": int(m_pooled.nobs),
                "inverted_u": inverted_u_pooled,
                "turning_point": turning_point_pooled,
            },
            "fixed_effects": {
                "beta_log_gdp": beta1_fe,
                "beta_log_gdp_sq": beta2_fe,
                "se_log_gdp": float(m_fe.bse[1]),
                "se_log_gdp_sq": float(m_fe.bse[2]),
                "pval_log_gdp": float(m_fe.pvalues[1]),
                "pval_log_gdp_sq": float(m_fe.pvalues[2]),
                "r_sq": float(m_fe.rsquared),
                "n_obs": int(m_fe.nobs),
                "n_countries": len(country_map),
                "inverted_u": inverted_u_fe,
                "turning_point": turning_point_fe,
                "quadratic_pval": f_test_pval,
            },
            "target": target_analysis,
            "country_iso3": country_iso3,
        }

        return {"score": score, "results": results}
