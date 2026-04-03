"""Learning outcomes per education dollar.

Constructs an efficiency ratio of measurable learning outcomes (PISA-equivalent
test scores, literacy rates, or gross enrollment) relative to public education
expenditure as a share of GDP. A higher ratio indicates more learning produced
per unit of public spending.

References:
    Hanushek, E.A. & Woessmann, L. (2012). Do better schools lead to more
        growth? Cognitive skills, economic outcomes, and causation.
        Journal of Economic Growth, 17(4), 267-321.
    UNESCO (2021). Global Education Monitoring Report.

Score: low efficiency (spending high, outcomes low) -> STRESS; high efficiency
(spending moderate, outcomes strong) -> STABLE.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class EducationExpenditureEfficiency(LayerBase):
    layer_id = "lED"
    name = "Education Expenditure Efficiency"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        # Education expenditure as % of GDP (SE.XPD.TOTL.GD.ZS)
        exp_rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SE.XPD.TOTL.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        # Literacy rate adult total % (SE.ADT.LITR.ZS) as proxy for outcomes
        lit_rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SE.ADT.LITR.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not exp_rows or not lit_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient expenditure or literacy data",
            }

        exp_vals = [r["value"] for r in exp_rows if r["value"] is not None]
        lit_vals = [r["value"] for r in lit_rows if r["value"] is not None]

        if not exp_vals or not lit_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid values"}

        expenditure_pct = exp_vals[0]
        literacy_rate = lit_vals[0]

        if expenditure_pct <= 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "zero expenditure"}

        efficiency_ratio = literacy_rate / expenditure_pct

        # Benchmark: ~15 literacy points per % GDP is reasonable
        # Higher ratio = better efficiency = lower stress score
        if efficiency_ratio >= 20:
            score = 15.0
        elif efficiency_ratio >= 15:
            score = 28.0
        elif efficiency_ratio >= 10:
            score = 45.0
        elif efficiency_ratio >= 6:
            score = 62.0
        else:
            score = 78.0

        return {
            "score": round(score, 2),
            "country": country,
            "education_expenditure_pct_gdp": round(expenditure_pct, 2),
            "literacy_rate_pct": round(literacy_rate, 2),
            "efficiency_ratio": round(efficiency_ratio, 3),
            "interpretation": "literacy rate per percentage point of GDP spent on education",
        }
