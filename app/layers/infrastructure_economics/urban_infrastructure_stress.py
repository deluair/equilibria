"""Urban Infrastructure Stress module.

Measures urban infrastructure adequacy relative to urban population demand.
Rapid urbanization without matching infrastructure investment creates acute stress.

Sources: WDI SP.URB.TOTL.IN.ZS (urban population % of total),
         WDI SP.URB.GROW (urban population growth rate, annual %),
         WDI NE.GDI.FTOT.ZS (GFCF % of GDP as investment proxy).
Score = clip(urb_growth * 10 - investment_adequacy * 5, 0, 100).
High urban growth + low investment -> high stress.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class UrbanInfrastructureStress(LayerBase):
    layer_id = "lIF"
    name = "Urban Infrastructure Stress"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        urb_grow_rows = await db.fetch_all(
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

        gfcf_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.GDI.FTOT.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not urb_grow_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        urb_growth = float(urb_grow_rows[0]["value"])
        gfcf = float(gfcf_rows[0]["value"]) if gfcf_rows else 20.0  # global avg fallback

        # Investment adequacy: normalized 0-1 where 30% GFCF = 1.0 (high investment)
        investment_adequacy = min(gfcf / 30.0, 1.0)
        raw = urb_growth * 15.0 - investment_adequacy * 20.0
        score = float(np.clip(raw, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "urban_growth_rate_pct": round(urb_growth, 3),
            "gfcf_pct_gdp": round(gfcf, 2),
            "investment_adequacy_index": round(investment_adequacy, 3),
            "interpretation": (
                "Acute urban infrastructure stress: rapid growth far outpacing investment"
                if score > 60
                else "High urban stress" if score > 40
                else "Moderate urban infrastructure pressure" if score > 20
                else "Urban infrastructure keeping pace with growth"
            ),
            "_sources": ["WDI:SP.URB.GROW", "WDI:NE.GDI.FTOT.ZS"],
        }
