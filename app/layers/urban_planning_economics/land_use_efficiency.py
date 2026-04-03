"""Land Use Efficiency module.

Measures how efficiently urban land is utilized relative to population density.
High urbanization with low density signals inefficient land use (sprawl).

Sources: WDI SP.URB.TOTL.IN.ZS (urban pop % of total), EN.POP.DNST (population density).
Score = clip(urb_share / (density_ratio + 1) * scaling, 0, 100).
Low density relative to urbanization level = high inefficiency score.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class LandUseEfficiency(LayerBase):
    layer_id = "lUP"
    name = "Land Use Efficiency"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        urb_rows = await db.fetch_all(
            """
            SELECT dp.value FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'SP.URB.TOTL.IN.ZS'
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

        if not urb_rows or not density_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data for land use efficiency"}

        urb_share = float(urb_rows[0]["value"])
        density = float(density_rows[0]["value"])

        if density <= 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "invalid density value"}

        # High urbanization + low density = inefficiency (sprawl)
        # Normalize density to a 0-1 scale (log scale, cap at 5000 ppl/km2)
        density_norm = np.log1p(density) / np.log1p(5000)
        # Inefficiency = high urban share with low density
        inefficiency = (urb_share / 100) / (density_norm + 0.05)
        score = float(np.clip(inefficiency * 40, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "urban_share_pct": round(urb_share, 2),
            "population_density_per_km2": round(density, 2),
            "interpretation": (
                "High sprawl risk: urbanized but low-density settlement pattern"
                if score > 65
                else "Moderate land use inefficiency"
                if score > 35
                else "Efficient or compact urban land use"
            ),
            "_sources": ["WDI:SP.URB.TOTL.IN.ZS", "WDI:EN.POP.DNST"],
        }
