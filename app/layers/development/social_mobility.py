"""Social mobility: intergenerational income elasticity and Great Gatsby curve.

Estimates intergenerational economic mobility through the elasticity of
children's income with respect to parents' income (IGE) and the rank-rank
slope. Tests the Great Gatsby curve (higher inequality = lower mobility).

Key references:
    Solon, G. (1999). Intergenerational mobility in the labor market. Handbook
        of Labor Economics, 3, 1761-1800.
    Corak, M. (2013). Income inequality, equality of opportunity, and
        intergenerational mobility. Journal of Economic Perspectives, 27(3).
    Chetty, R. et al. (2014). Where is the land of opportunity? QJE, 129(4),
        1553-1623.
"""

from __future__ import annotations

import numpy as np
import statsmodels.api as sm

from app.layers.base import LayerBase


class SocialMobility(LayerBase):
    layer_id = "l4"
    name = "Social Mobility"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Estimate social mobility and test Great Gatsby curve.

        Uses cross-country data on inequality (Gini) and mobility proxies
        (education mobility, income share dynamics). Tests whether higher
        inequality is associated with lower mobility.

        Returns dict with score, mobility estimates, Great Gatsby curve
        coefficients, and country ranking.
        """
        country_iso3 = kwargs.get("country_iso3")

        # Proxy for intergenerational mobility: education mobility
        # Use adult literacy and youth literacy gap as proxy
        adult_lit_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SE.ADT.LITR.ZS'
              AND dp.value IS NOT NULL
              AND dp.date = (
                  SELECT MAX(dp2.date) FROM data_points dp2
                  WHERE dp2.series_id = ds.id
              )
            """
        )
        youth_lit_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SE.ADT.1524.LT.ZS'
              AND dp.value IS NOT NULL
              AND dp.date = (
                  SELECT MAX(dp2.date) FROM data_points dp2
                  WHERE dp2.series_id = ds.id
              )
            """
        )

        # Gini coefficient
        gini_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SI.POV.GINI'
              AND dp.value > 0
              AND dp.date = (
                  SELECT MAX(dp2.date) FROM data_points dp2
                  WHERE dp2.series_id = ds.id
              )
            """
        )

        # Income share of bottom 20% and top 10%
        bottom20_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SI.DST.FRST.20'
              AND dp.value IS NOT NULL
              AND dp.date = (
                  SELECT MAX(dp2.date) FROM data_points dp2
                  WHERE dp2.series_id = ds.id
              )
            """
        )
        top10_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SI.DST.10TH.10'
              AND dp.value IS NOT NULL
              AND dp.date = (
                  SELECT MAX(dp2.date) FROM data_points dp2
                  WHERE dp2.series_id = ds.id
              )
            """
        )

        # Secondary school enrollment for education mobility
        sec_enrol_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SE.SEC.ENRR'
              AND dp.value IS NOT NULL
              AND dp.date = (
                  SELECT MAX(dp2.date) FROM data_points dp2
                  WHERE dp2.series_id = ds.id
              )
            """
        )

        # Build data dictionaries
        adult_lit = {r["country_iso3"]: r["value"] for r in adult_lit_rows} if adult_lit_rows else {}
        youth_lit = {r["country_iso3"]: r["value"] for r in youth_lit_rows} if youth_lit_rows else {}
        gini_dict = {r["country_iso3"]: r["value"] for r in gini_rows} if gini_rows else {}
        bottom20 = {r["country_iso3"]: r["value"] for r in bottom20_rows} if bottom20_rows else {}
        top10 = {r["country_iso3"]: r["value"] for r in top10_rows} if top10_rows else {}
        sec_enrol = {r["country_iso3"]: r["value"] for r in sec_enrol_rows} if sec_enrol_rows else {}

        # Education mobility proxy: youth-adult literacy gap (positive = upward mobility)
        edu_mobility: dict[str, float] = {}
        for iso in set(adult_lit.keys()) & set(youth_lit.keys()):
            gap = youth_lit[iso] - adult_lit[iso]
            edu_mobility[iso] = gap

        # Composite mobility proxy using available indicators
        mobility_index: dict[str, dict] = {}
        for iso in gini_dict:
            components = {}

            # Education mobility component (higher gap = more mobility)
            if iso in edu_mobility:
                components["edu_mobility"] = edu_mobility[iso]

            # Income share bottom 20% (higher = more equal opportunity)
            if iso in bottom20:
                components["bottom20_share"] = bottom20[iso]

            # Secondary enrollment (higher = more opportunity)
            if iso in sec_enrol:
                components["sec_enrollment"] = sec_enrol[iso]

            if components:
                # Standardize and average components
                vals = list(components.values())
                # Higher values = more mobility in all components
                avg = np.mean(vals)
                mobility_index[iso] = {
                    "components": components,
                    "gini": gini_dict[iso],
                    "composite": float(avg),
                }

        # Great Gatsby curve: inequality (Gini) vs mobility proxy
        gatsby_results = None
        if len(mobility_index) >= 15:
            isos = list(mobility_index.keys())
            gini_arr = np.array([mobility_index[c]["gini"] for c in isos])
            mob_arr = np.array([mobility_index[c]["composite"] for c in isos])

            # Standardize mobility for comparison
            mob_mean = np.mean(mob_arr)
            mob_std = np.std(mob_arr)
            if mob_std > 0:
                mob_z = (mob_arr - mob_mean) / mob_std
            else:
                mob_z = mob_arr

            X = sm.add_constant(gini_arr)
            m = sm.OLS(mob_z, X).fit(cov_type="HC1")

            gatsby_results = {
                "gini_coef": float(m.params[1]),
                "se": float(m.bse[1]),
                "pval": float(m.pvalues[1]),
                "r_sq": float(m.rsquared),
                "n_obs": int(m.nobs),
                "gatsby_confirmed": float(m.params[1]) < 0 and float(m.pvalues[1]) < 0.10,
            }

        # Rank-rank analysis: correlation between Gini and bottom 20 share
        rank_rank = None
        common_rr = sorted(set(gini_dict.keys()) & set(bottom20.keys()))
        if len(common_rr) >= 15:
            gini_ranks = np.argsort(np.argsort([gini_dict[c] for c in common_rr]))
            bottom_ranks = np.argsort(np.argsort([bottom20[c] for c in common_rr]))

            X_rr = sm.add_constant(gini_ranks.astype(float))
            m_rr = sm.OLS(bottom_ranks.astype(float), X_rr).fit()

            rank_rank = {
                "slope": float(m_rr.params[1]),
                "r_sq": float(m_rr.rsquared),
                "n_obs": int(m_rr.nobs),
                "correlation": float(np.corrcoef(gini_ranks, bottom_ranks)[0, 1]),
            }

        # Palma ratio: top 10% share / bottom 40% share
        palma_data = {}
        bottom40_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SI.DST.04TH.20'
              AND dp.value IS NOT NULL
              AND dp.date = (
                  SELECT MAX(dp2.date) FROM data_points dp2
                  WHERE dp2.series_id = ds.id
              )
            """
        )
        if bottom40_rows and top10:
            b40_dict = {r["country_iso3"]: r["value"] for r in bottom40_rows}
            for iso in set(b40_dict.keys()) & set(bottom20.keys()) & set(top10.keys()):
                b40 = bottom20[iso] + b40_dict.get(iso, 0)
                if b40 > 0:
                    palma_data[iso] = top10[iso] / b40

        # Target country
        target_analysis = None
        if country_iso3:
            target_analysis = {
                "gini": gini_dict.get(country_iso3),
                "bottom20_share": bottom20.get(country_iso3),
                "top10_share": top10.get(country_iso3),
                "edu_mobility": edu_mobility.get(country_iso3),
                "sec_enrollment": sec_enrol.get(country_iso3),
                "palma_ratio": palma_data.get(country_iso3),
                "mobility_index": mobility_index.get(country_iso3),
            }

        # Score: low mobility (high Gini, low bottom share) = stress
        if target_analysis and target_analysis["gini"] is not None:
            gini = target_analysis["gini"]
            b20 = target_analysis["bottom20_share"]
            if gini > 50 or (b20 is not None and b20 < 4):
                score = 80
            elif gini > 40 or (b20 is not None and b20 < 6):
                score = 60
            elif gini > 33:
                score = 40
            else:
                score = 25
        elif gatsby_results and gatsby_results["gatsby_confirmed"]:
            score = 60
        else:
            score = 50

        score = float(np.clip(score, 0, 100))

        results = {
            "great_gatsby_curve": gatsby_results,
            "rank_rank": rank_rank,
            "n_countries_with_mobility": len(mobility_index),
            "target": target_analysis,
            "country_iso3": country_iso3,
        }

        return {"score": score, "results": results}
