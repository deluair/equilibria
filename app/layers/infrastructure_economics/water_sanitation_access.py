"""Water and Sanitation Access module.

Measures WASH (Water, Sanitation and Hygiene) coverage gap.
Lack of safe water and basic sanitation undermines health and productivity.

Sources: WDI SH.H2O.BASW.ZS (people using at least basic drinking water, %),
         WDI SH.STA.BASS.ZS (people using at least basic sanitation services, %).
Score = (water_gap * 0.5 + sanit_gap * 0.5), clipped 0-100.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class WaterSanitationAccess(LayerBase):
    layer_id = "lIF"
    name = "Water and Sanitation Access"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        water_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SH.H2O.BASW.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        sanit_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SH.STA.BASS.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not water_rows and not sanit_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        water_access = float(water_rows[0]["value"]) if water_rows else None
        sanit_access = float(sanit_rows[0]["value"]) if sanit_rows else None

        water_gap = (100.0 - water_access) if water_access is not None else None
        sanit_gap = (100.0 - sanit_access) if sanit_access is not None else None

        if water_gap is not None and sanit_gap is not None:
            score = float(np.clip(water_gap * 0.5 + sanit_gap * 0.5, 0, 100))
        elif water_gap is not None:
            score = float(np.clip(water_gap, 0, 100))
        else:
            score = float(np.clip(sanit_gap, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "water_access_pct": round(water_access, 2) if water_access is not None else None,
            "sanitation_access_pct": round(sanit_access, 2) if sanit_access is not None else None,
            "water_gap_ppt": round(water_gap, 2) if water_gap is not None else None,
            "sanitation_gap_ppt": round(sanit_gap, 2) if sanit_gap is not None else None,
            "interpretation": (
                "Critical WASH deficit: severe public health and productivity risk"
                if score > 50
                else "Substantial WASH gap" if score > 25
                else "Partial WASH coverage gap" if score > 10
                else "Near-universal WASH coverage"
            ),
            "_sources": ["WDI:SH.H2O.BASW.ZS", "WDI:SH.STA.BASS.ZS"],
        }
