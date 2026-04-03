"""Intergenerational educational mobility.

Measures the degree to which children's educational attainment is independent
of parental educational attainment. High immobility (low mobility) signals
entrenched inequality of opportunity and perpetuation of human capital gaps
across generations.

Indicator: intergenerational education persistence coefficient (IGE_edu),
estimated as regression coefficient of child's years of schooling on parent's
years of schooling. IGE close to 1 = immobile, IGE close to 0 = fully mobile.

Also uses the World Bank GDIM (Global Database on Intergenerational Mobility)
upward mobility rate: probability a child born to parents with no post-primary
education obtains post-primary education.

References:
    Narayan, A. et al. (2018). Fair Progress? Economic Mobility Across
        Generations Around the World. World Bank Equity and Development Series.
    Corak, M. (2013). Income inequality, equality of opportunity, and
        intergenerational mobility. JEP, 27(3), 79-102.
    Hertz, T. et al. (2007). The inheritance of educational inequality:
        international comparisons and fifty-year trends. B.E. Journal of
        Economic Analysis & Policy, 7(2).

Score: high immobility (high IGE) -> STRESS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class EducationMobilityIndex(LayerBase):
    layer_id = "lED"
    name = "Education Mobility Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        # IGE education coefficient (0=fully mobile, 1=perfectly immobile)
        ige_rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('gdim_ige', 'education_mobility')
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        # Upward mobility rate from GDIM (probability of achieving post-primary)
        upward_rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'gdim_upward_mobility'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        ige = None
        upward_mobility = None

        ige_vals = [r["value"] for r in ige_rows if r["value"] is not None]
        if ige_vals:
            ige = ige_vals[0]

        up_vals = [r["value"] for r in upward_rows if r["value"] is not None]
        if up_vals:
            upward_mobility = up_vals[0]

        if ige is None and upward_mobility is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no intergenerational mobility data",
            }

        # Score from IGE (primary indicator)
        if ige is not None:
            ige = max(0.0, min(1.0, ige))
            score_ige = ige * 100.0
        else:
            score_ige = None

        # Score from upward mobility (secondary): high upward mobility = low stress
        if upward_mobility is not None:
            upward_mobility = max(0.0, min(100.0, upward_mobility))
            score_upward = 100.0 - upward_mobility
        else:
            score_upward = None

        # Combine
        components = [s for s in [score_ige, score_upward] if s is not None]
        score = sum(components) / len(components)

        return {
            "score": round(score, 2),
            "country": country,
            "ige_education_coefficient": round(ige, 4) if ige is not None else None,
            "upward_mobility_rate_pct": round(upward_mobility, 2) if upward_mobility is not None else None,
            "mobility_classification": (
                "low" if score >= 65 else "medium" if score >= 35 else "high"
            ),
            "interpretation": "IGE close to 1 = immobile; upward_mobility_rate = prob of escaping parental low education",
        }
