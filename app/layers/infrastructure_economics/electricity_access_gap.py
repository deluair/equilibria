"""Electricity Access Gap module.

Measures the share of population without access to reliable electricity.
Lack of electricity access constrains productivity, health, and education.

Sources: WDI EG.ELC.ACCS.ZS (access to electricity, % of population).
Score = clip(100 - elec_access_pct, 0, 100).
Score=0 means universal access; Score=100 means no access.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ElectricityAccessGap(LayerBase):
    layer_id = "lIF"
    name = "Electricity Access Gap"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'EG.ELC.ACCS.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        elec_access = float(rows[0]["value"])
        gap = float(np.clip(100.0 - elec_access, 0, 100))

        return {
            "score": round(gap, 1),
            "country": country,
            "electricity_access_pct": round(elec_access, 2),
            "population_without_access_pct": round(gap, 2),
            "reference_year": str(rows[0]["date"]),
            "interpretation": (
                "Severe electricity access deficit: major barrier to development"
                if gap > 40
                else "Significant portion of population lacks electricity" if gap > 20
                else "Partial coverage gap" if gap > 5
                else "Near-universal electricity access"
            ),
            "_sources": ["WDI:EG.ELC.ACCS.ZS"],
        }
