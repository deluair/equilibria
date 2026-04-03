"""Urban Sprawl Index module.

Measures decoupling between urban population growth and economic growth.
When urban expansion outpaces GDP growth, it signals inefficient land-consuming sprawl.

Sources: WDI SP.URB.GROW (urban population growth rate %), NY.GDP.MKTP.KD.ZG (GDP growth rate %).
Score = clip(max(0, urb_growth - gdp_growth) * 20, 0, 100).
Urban growth >> GDP growth = sprawl; urban growth << GDP growth = densification.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class UrbanSprawlIndex(LayerBase):
    layer_id = "lUP"
    name = "Urban Sprawl Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        urb_grow_rows = await db.fetch_all(
            """
            SELECT dp.value FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'SP.URB.GROW'
            ORDER BY dp.date DESC LIMIT 15
            """,
            (country,),
        )

        gdp_grow_rows = await db.fetch_all(
            """
            SELECT dp.value FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY dp.date DESC LIMIT 15
            """,
            (country,),
        )

        if not urb_grow_rows or not gdp_grow_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data for urban sprawl index"}

        urb_growth = float(urb_grow_rows[0]["value"])
        gdp_growth = float(gdp_grow_rows[0]["value"])

        # Sprawl = urban growth exceeds economic growth (uncoupled expansion)
        decoupling = urb_growth - gdp_growth
        score = float(np.clip(max(0.0, decoupling) * 20, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "urban_growth_rate_pct": round(urb_growth, 3),
            "gdp_growth_rate_pct": round(gdp_growth, 3),
            "decoupling_gap_ppt": round(decoupling, 3),
            "interpretation": (
                "Severe sprawl risk: urban expansion far outpacing economic growth"
                if decoupling > 3
                else "Moderate decoupling: some urban sprawl pressure"
                if decoupling > 1
                else "Urban and economic growth broadly coupled"
                if decoupling >= 0
                else "Urban densification relative to economic activity"
            ),
            "_sources": ["WDI:SP.URB.GROW", "WDI:NY.GDP.MKTP.KD.ZG"],
        }
