"""Knowledge Complexity module.

Knowledge accumulation proxy using R&D expenditure and tertiary education enrollment.
Low knowledge stock = high complexity gap = high stress.

Score = 100 - normalized_composite (0-100 where composite is normalized R&D + tertiary)

Sources: WDI GB.XPD.RSDV.GD.ZS (R&D % GDP), SE.TER.ENRR (tertiary enrollment rate)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class KnowledgeComplexity(LayerBase):
    layer_id = "lCP"
    name = "Knowledge Complexity"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rnd_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GB.XPD.RSDV.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
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
            LIMIT 5
            """,
            (country,),
        )

        if not rnd_rows and not edu_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        # R&D % GDP: global high ~4-5%, most >3% = strong. Normalize 0-5%.
        if rnd_rows:
            rnd_val = float(rnd_rows[0]["value"])
            rnd_norm = min(1.0, rnd_val / 5.0)
            rnd_date = rnd_rows[0]["date"]
        else:
            rnd_norm = 0.0
            rnd_val = None
            rnd_date = None

        # Tertiary enrollment: 0-100+%. Cap at 100.
        if edu_rows:
            edu_val = float(edu_rows[0]["value"])
            edu_norm = min(1.0, edu_val / 100.0)
            edu_date = edu_rows[0]["date"]
        else:
            edu_norm = 0.0
            edu_val = None
            edu_date = None

        # Composite: equal weight where available
        components = [v for v in [rnd_norm, edu_norm] if v is not None]
        composite_norm = float(np.mean(components))

        score = float(max(0.0, min(100.0, (1.0 - composite_norm) * 100.0)))

        return {
            "score": round(score, 1),
            "country": country,
            "rnd_pct_gdp": round(rnd_val, 3) if rnd_val is not None else None,
            "rnd_norm_0_1": round(rnd_norm, 4),
            "rnd_date": rnd_date,
            "tertiary_enrollment_rate_pct": round(edu_val, 2) if edu_val is not None else None,
            "edu_norm_0_1": round(edu_norm, 4),
            "edu_date": edu_date,
            "composite_knowledge_norm": round(composite_norm, 4),
            "interpretation": (
                "High score = low knowledge stock (low R&D + low tertiary) = complexity gap. "
                "Low score = high knowledge accumulation."
            ),
            "_citation": "World Bank WDI: GB.XPD.RSDV.GD.ZS, SE.TER.ENRR",
        }
