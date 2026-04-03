"""Language and Skill Gap module.

Estimates the human capital gap faced by migrants in the host country
using adult literacy rate and tertiary education enrollment. Low
literacy and low higher education enrollment signal that the workforce
(including immigrant arrivals) may lack skills for labor market
integration, creating persistent wage penalties and occupational
downgrading.

Score = clip(100 - (literacy_component + edu_component), 0, 100)

Sources: WDI (SE.ADT.LITR.ZS, SE.TER.ENRR)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class LanguageSkillGap(LayerBase):
    layer_id = "lMI"
    name = "Language and Skill Gap"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        literacy_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SE.ADT.LITR.ZS'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        edu_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SE.TER.ENRR'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        if not literacy_rows and not edu_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        lit_vals = [float(r["value"]) for r in literacy_rows if r["value"] is not None]
        edu_vals = [float(r["value"]) for r in edu_rows if r["value"] is not None]

        if not lit_vals and not edu_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        literacy_rate = float(np.mean(lit_vals)) if lit_vals else 80.0
        tertiary_enrr = float(np.mean(edu_vals)) if edu_vals else 30.0

        # Higher literacy = better integration capacity (reduces gap score)
        lit_component = float(np.clip(literacy_rate / 2, 0, 50))
        # Higher tertiary enrollment = better skill match (reduces gap score)
        edu_component = float(np.clip(tertiary_enrr / 2, 0, 50))

        # Gap score: low human capital = high gap = high score
        raw_capacity = lit_component + edu_component
        score = float(np.clip(100 - raw_capacity, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "adult_literacy_rate_avg_pct": round(literacy_rate, 2),
            "tertiary_enrollment_avg_pct": round(tertiary_enrr, 2),
            "components": {
                "literacy_capacity": round(lit_component, 2),
                "education_capacity": round(edu_component, 2),
            },
            "n_obs_literacy": len(lit_vals),
            "n_obs_education": len(edu_vals),
            "interpretation": (
                "large skill gap" if score > 65
                else "moderate gap" if score > 35
                else "small skill gap"
            ),
        }
