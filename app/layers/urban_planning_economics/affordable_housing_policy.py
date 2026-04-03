"""Affordable Housing Policy module.

Measures adequacy of housing policy by combining slum prevalence with urban growth pressure.
Rapid urban growth alongside high slum shares indicates failing affordable housing policy.

Sources: WDI EN.POP.SLUM.UR.ZS (slum population %), SP.URB.GROW (urban growth rate %).
Score = clip(slum_pct * 0.7 + max(0, urb_growth - 2) * 10, 0, 100).
High slum share + rapid urban growth = acute housing policy failure.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class AffordableHousingPolicy(LayerBase):
    layer_id = "lUP"
    name = "Affordable Housing Policy"

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

        urb_grow_rows = await db.fetch_all(
            """
            SELECT dp.value FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'SP.URB.GROW'
            ORDER BY dp.date DESC LIMIT 15
            """,
            (country,),
        )

        if not slum_rows and not urb_grow_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no housing policy data available"}

        slum_pct = float(slum_rows[0]["value"]) if slum_rows else None
        urb_growth = float(urb_grow_rows[0]["value"]) if urb_grow_rows else None

        if slum_pct is None and urb_growth is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for affordable housing policy"}

        score_components = []
        if slum_pct is not None:
            score_components.append(slum_pct * 0.7)
        if urb_growth is not None:
            score_components.append(max(0.0, urb_growth - 2.0) * 10)

        score = float(np.clip(sum(score_components), 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "slum_population_pct": round(slum_pct, 2) if slum_pct is not None else None,
            "urban_growth_rate_pct": round(urb_growth, 3) if urb_growth is not None else None,
            "interpretation": (
                "Critical housing policy failure: high informality and rapid urban growth"
                if score > 65
                else "Significant affordable housing gap"
                if score > 35
                else "Housing policy broadly adequate relative to urbanization pace"
            ),
            "_sources": ["WDI:EN.POP.SLUM.UR.ZS", "WDI:SP.URB.GROW"],
        }
