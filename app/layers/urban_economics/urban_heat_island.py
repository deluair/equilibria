"""Urban Heat Island module.

Measures climate-urban interaction risk. High urbanization combined with
high per-capita CO2 emissions increases urban heat island intensity,
amplifying energy demand, health risks, and cooling costs.

Sources: WDI SP.URB.TOTL.IN.ZS (urban pop % of total),
         WDI EN.ATM.CO2E.PC (CO2 emissions, metric tons per capita).
Score = clip((urban_pct/100) * (co2_pc/10) * 50, 0, 100).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class UrbanHeatIsland(LayerBase):
    layer_id = "lUE"
    name = "Urban Heat Island"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        urb_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.URB.TOTL.IN.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        co2_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'EN.ATM.CO2E.PC'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not urb_rows or not co2_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        urban_pct = float(urb_rows[0]["value"])
        co2_pc = float(co2_rows[0]["value"])

        score = float(np.clip((urban_pct / 100.0) * (co2_pc / 10.0) * 50.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "urban_share_pct": round(urban_pct, 2),
            "co2_per_capita_tonnes": round(co2_pc, 3),
            "heat_island_composite": round((urban_pct / 100.0) * (co2_pc / 10.0), 4),
            "interpretation": (
                "High urban heat island risk: dense urban cover with high emissions"
                if score > 50
                else "Moderate heat island stress" if score > 25
                else "Low urban heat island exposure"
            ),
            "_sources": ["WDI:SP.URB.TOTL.IN.ZS", "WDI:EN.ATM.CO2E.PC"],
        }
