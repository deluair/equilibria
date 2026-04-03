"""Public Space Access module.

Measures urban public space accessibility using slum population as inverse proxy.
High slum share indicates residents lack formal public space and infrastructure access.

Sources: WDI EN.POP.SLUM.UR.ZS (slum population % of urban population).
Score = clip(slum_pct * 1.1, 0, 100) — inverted: higher slum share = worse access.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PublicSpaceAccess(LayerBase):
    layer_id = "lUP"
    name = "Public Space Access"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        slum_rows = await db.fetch_all(
            """
            SELECT dp.value FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'EN.POP.SLUM.UR.ZS'
            ORDER BY dp.date DESC LIMIT 15
            """,
            (country,),
        )

        if not slum_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no slum population data for public space access"}

        slum_pct = float(slum_rows[0]["value"])

        # Higher slum share = lower public space access = higher stress score
        score = float(np.clip(slum_pct * 1.1, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "slum_population_pct": round(slum_pct, 2),
            "public_space_access_gap": round(score, 1),
            "interpretation": (
                "Critical public space deficit: majority of urban poor lack formal infrastructure"
                if slum_pct > 60
                else "Significant public space access inequality"
                if slum_pct > 25
                else "Moderate informal settlement share"
                if slum_pct > 10
                else "Relatively good urban public space provision"
            ),
            "_sources": ["WDI:EN.POP.SLUM.UR.ZS"],
        }
