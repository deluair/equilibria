"""Housing Urban Stress module.

Measures urban housing stress from the combination of rapid urbanization
and high informal settlement prevalence. Both high simultaneously indicates
severe housing crisis and unmet demand.

Sources: WDI SP.URB.GROW (urban population growth, % per year),
         WDI EN.POP.SLUM.UR.ZS (slum population, % of urban).
Score = clip(urb_growth * slum_share / 10, 0, 100).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HousingUrbanStress(LayerBase):
    layer_id = "lUE"
    name = "Housing Urban Stress"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        growth_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.URB.GROW'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        slum_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'EN.POP.SLUM.UR.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not growth_rows or not slum_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        urb_growth = float(growth_rows[0]["value"])
        slum_share = float(slum_rows[0]["value"])

        score = float(np.clip(urb_growth * slum_share / 10.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "urban_growth_rate_pct": round(urb_growth, 3),
            "slum_share_pct": round(slum_share, 2),
            "stress_composite": round(urb_growth * slum_share / 10.0, 3),
            "interpretation": (
                "Severe housing stress: rapid urbanization into already informal settlements"
                if score > 50
                else "Significant housing pressure" if score > 25
                else "Moderate housing-urban risk" if score > 10
                else "Low housing-urban stress"
            ),
            "_sources": ["WDI:SP.URB.GROW", "WDI:EN.POP.SLUM.UR.ZS"],
        }
