"""Building Code Compliance module.

Measures the institutional capacity to enforce building codes using regulatory quality
and climate policy stringency as proxies. Weak governance and low policy ambition
indicate poor code compliance environments.

Sources: WGI RQ.EST (regulatory quality, -2.5 to +2.5),
         WDI EN.CLC.MDAT.ZS (countries with climate legislation %, binary-ish proxy).
Score = clip((1 - rq_norm) * 70 + low_climate_policy * 30, 0, 100).
Poor regulation + weak climate commitment = high non-compliance risk.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class BuildingCodeCompliance(LayerBase):
    layer_id = "lUP"
    name = "Building Code Compliance"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rq_rows = await db.fetch_all(
            """
            SELECT dp.value FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'RQ.EST'
            ORDER BY dp.date DESC LIMIT 15
            """,
            (country,),
        )

        climate_rows = await db.fetch_all(
            """
            SELECT dp.value FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'EN.CLC.MDAT.ZS'
            ORDER BY dp.date DESC LIMIT 15
            """,
            (country,),
        )

        if not rq_rows and not climate_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no regulatory data for building code compliance"}

        score_components = []
        rq_val = None
        climate_val = None

        if rq_rows:
            # RQ.EST: -2.5 to +2.5. Normalize to 0-1. Higher = better regulation.
            rq_raw = float(rq_rows[0]["value"])
            rq_val = round(rq_raw, 3)
            rq_norm = float(np.clip((rq_raw + 2.5) / 5.0, 0, 1))
            # Poor regulation = higher non-compliance risk (weighted 70%)
            score_components.append((1.0 - rq_norm) * 70.0)

        if climate_rows:
            # EN.CLC.MDAT.ZS: percentage with climate mandates. Higher = stronger policy.
            climate_raw = float(climate_rows[0]["value"])
            climate_val = round(climate_raw, 2)
            climate_norm = float(np.clip(climate_raw / 100.0, 0, 1))
            # Weak climate policy = higher non-compliance risk (weighted 30%)
            score_components.append((1.0 - climate_norm) * 30.0)

        if not score_components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable data for building code compliance"}

        score = float(np.clip(sum(score_components), 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "regulatory_quality_wgi": rq_val,
            "climate_mandate_coverage_pct": climate_val,
            "interpretation": (
                "Weak institutional capacity: high building code non-compliance risk"
                if score > 65
                else "Moderate regulatory gaps in building code enforcement"
                if score > 35
                else "Strong regulatory environment for building code compliance"
            ),
            "_sources": ["WGI:RQ.EST", "WDI:EN.CLC.MDAT.ZS"],
        }
