"""STEM Capacity module.

Proxies the availability of science, technology, engineering, and mathematics
human capital using:
- Researchers in R&D per million people (SP.POP.SCIE.RD.P6) -- preferred
- Gross tertiary school enrollment rate (SE.TER.ENRR) -- fallback

When researcher density data is available, it is combined with tertiary
enrollment. If only one series is available, it is used alone. Low STEM
capacity signals an innovation input gap that constrains long-run growth.

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class STEMCapacity(LayerBase):
    layer_id = "lNV"
    name = "STEM Capacity"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        researcher_rows = await db.fetch_all(
            """
            SELECT dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.POP.SCIE.RD.P6'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        tertiary_rows = await db.fetch_all(
            """
            SELECT dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SE.TER.ENRR'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        researchers: float | None = None
        tertiary: float | None = None

        if researcher_rows:
            vals = [float(r["value"]) for r in researcher_rows if r["value"] is not None]
            researchers = float(np.mean(vals)) if vals else None

        if tertiary_rows:
            vals = [float(r["value"]) for r in tertiary_rows if r["value"] is not None]
            tertiary = float(np.mean(vals)) if vals else None

        if researchers is None and tertiary is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        normed_components: list[float] = []
        if researchers is not None:
            # Researchers per million: 0-8000 -> 0-100
            normed_components.append(min(100.0, (researchers / 8000.0) * 100.0))
        if tertiary is not None:
            # Tertiary enrollment: 0-100% -> 0-100
            normed_components.append(min(100.0, max(0.0, tertiary)))

        stem_composite = float(np.mean(normed_components))
        score = max(0.0, 100.0 - stem_composite)

        return {
            "score": round(score, 1),
            "country": country,
            "researchers_per_million": round(researchers, 1) if researchers is not None else None,
            "tertiary_enrollment_pct": round(tertiary, 2) if tertiary is not None else None,
            "stem_composite": round(stem_composite, 2),
            "n_dimensions": len(normed_components),
            "interpretation": "Low STEM composite = innovation input gap",
        }
