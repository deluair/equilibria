"""Intergenerational Mobility module.

Implements the Great Gatsby Curve (Corak 2013): countries with higher income
inequality tend to have lower intergenerational mobility. Tertiary enrollment
is used as a proxy for upward mobility opportunity.

Indicators:
  - SI.POV.GINI   : Gini coefficient (inequality)
  - SE.TER.ENRR   : tertiary school enrollment rate (% gross)

High Gini + low tertiary enrollment = low mobility trap = high stress.

Score = Gini component + (100 - enrollment) component, weighted and clipped.

Sources: WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class IntergenerationalMobility(LayerBase):
    layer_id = "lWE"
    name = "Intergenerational Mobility"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        series_map = {
            "gini": "SI.POV.GINI",
            "tertiary_enrollment": "SE.TER.ENRR",
        }

        latest: dict[str, float | None] = {}
        dates: dict[str, str | None] = {}

        for key, sid in series_map.items():
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country, sid),
            )
            if rows:
                latest[key] = float(rows[0]["value"])
                dates[key] = rows[0]["date"]
            else:
                latest[key] = None
                dates[key] = None

        available = {k: v for k, v in latest.items() if v is not None}
        if len(available) == 0:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no intergenerational mobility data available",
            }

        components: dict[str, float] = {}

        # Inequality penalty (max 55 points): Gini above 30
        if latest["gini"] is not None:
            components["inequality"] = float(np.clip((latest["gini"] - 30.0) * 1.1, 0, 55))

        # Low mobility proxy (max 45 points): low tertiary enrollment
        if latest["tertiary_enrollment"] is not None:
            # High enrollment -> low penalty. Enrollment above 60% -> minimal stress.
            components["low_mobility"] = float(np.clip((60.0 - latest["tertiary_enrollment"]) * 0.75, 0, 45))

        if not components:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data for mobility score",
            }

        score = float(np.clip(sum(components.values()), 0, 100))

        # Great Gatsby curve index: product of Gini * (1 - enr/100) normalized
        gatsby_index = None
        if latest["gini"] is not None and latest["tertiary_enrollment"] is not None:
            enr_norm = float(np.clip(latest["tertiary_enrollment"] / 100.0, 0, 1))
            gatsby_index = round(float(latest["gini"] / 100.0) * (1.0 - enr_norm), 4)

        return {
            "score": round(score, 1),
            "country": country,
            "gini": round(latest["gini"], 2) if latest["gini"] is not None else None,
            "gini_date": dates["gini"],
            "tertiary_enrollment_pct": round(latest["tertiary_enrollment"], 2) if latest["tertiary_enrollment"] is not None else None,
            "tertiary_enrollment_date": dates["tertiary_enrollment"],
            "gatsby_index": gatsby_index,
            "score_components": {k: round(v, 2) for k, v in components.items()},
            "method": "Great Gatsby Curve: Gini inequality + low tertiary enrollment mobility trap",
            "reference": "Corak 2013; Chetty et al. 2014; Miles Corak 'Income Inequality, Equality of Opportunity'",
        }
