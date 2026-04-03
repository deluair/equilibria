"""Urban Green Space Gap module.

Measures the deficit in urban green space by combining forest cover share with population
density. High-density countries with low forest cover face the greatest green space deficit.

Sources: WDI AG.LND.FRST.ZS (forest area % of land area), EN.POP.DNST (population density).
Score = clip((density_norm * (1 - forest_share)) * 100, 0, 100).
Dense and deforested = acute green space gap.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class UrbanGreenSpaceGap(LayerBase):
    layer_id = "lUP"
    name = "Urban Green Space Gap"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        forest_rows = await db.fetch_all(
            """
            SELECT dp.value FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'AG.LND.FRST.ZS'
            ORDER BY dp.date DESC LIMIT 15
            """,
            (country,),
        )

        density_rows = await db.fetch_all(
            """
            SELECT dp.value FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'EN.POP.DNST'
            ORDER BY dp.date DESC LIMIT 15
            """,
            (country,),
        )

        if not forest_rows or not density_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data for urban green space gap"}

        forest_pct = float(forest_rows[0]["value"])
        density = float(density_rows[0]["value"])

        if density <= 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "invalid density value"}

        # Normalize density on log scale (cap at 5000 ppl/km2)
        density_norm = float(np.clip(np.log1p(density) / np.log1p(5000), 0, 1))
        # Forest share as fraction (0-1)
        forest_share = np.clip(forest_pct / 100.0, 0, 1)
        # Gap: dense countries with low forest cover score highest
        score = float(np.clip(density_norm * (1.0 - forest_share) * 100, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "forest_area_pct": round(forest_pct, 2),
            "population_density_per_km2": round(density, 2),
            "interpretation": (
                "Critical green space deficit: dense urban fabric with negligible tree cover"
                if score > 65
                else "Significant green space pressure relative to population density"
                if score > 35
                else "Adequate green space relative to population density"
            ),
            "_sources": ["WDI:AG.LND.FRST.ZS", "WDI:EN.POP.DNST"],
        }
