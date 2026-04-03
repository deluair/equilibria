"""Intergenerational Inequality module.

Implements the Great Gatsby curve logic: countries with higher income inequality
(Gini) tend to have lower intergenerational social mobility.

High Gini + low tertiary enrollment = intergenerational trap, because:
- High inequality concentrates resources in few families.
- Low higher education access prevents children from climbing the ladder.

Formula:
    score = clip(gini/100 * (1 - tertiary/100) * 200, 0, 100)

This gives a product that penalizes the combination of both factors:
- Gini=50, tertiary=50 -> score = 0.5 * 0.5 * 200 = 50 (STRESS)
- Gini=60, tertiary=20 -> score = 0.6 * 0.8 * 200 = 96 (CRISIS)
- Gini=30, tertiary=80 -> score = 0.3 * 0.2 * 200 = 12 (STABLE)

Sources:
- SI.POV.GINI: Gini index (WDI)
- SE.TER.ENRR: School enrollment, tertiary (% gross, WDI)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class IntergenerationalInequality(LayerBase):
    layer_id = "lIQ"
    name = "Intergenerational Inequality"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        gini_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.POV.GINI'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        tertiary_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SE.TER.ENRR'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not gini_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        gini = float(gini_rows[0]["value"])
        tertiary = float(tertiary_rows[0]["value"]) if tertiary_rows else 30.0
        has_gini = True
        has_tertiary = bool(tertiary_rows)

        # Tertiary can exceed 100 (gross enrollment); cap at 100 for formula
        tertiary_capped = float(np.clip(tertiary, 0, 100))

        score = float(np.clip(gini / 100.0 * (1.0 - tertiary_capped / 100.0) * 200.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "gini": round(gini, 2),
            "tertiary_enrollment_pct": round(tertiary, 2),
            "tertiary_capped": round(tertiary_capped, 2),
            "gini_source": "observed" if has_gini else "imputed_default",
            "tertiary_source": "observed" if has_tertiary else "imputed_default",
            "interpretation": {
                "great_gatsby_trap": gini > 40 and tertiary_capped < 40,
                "high_gini": gini > 40,
                "low_tertiary": tertiary_capped < 40,
            },
        }
