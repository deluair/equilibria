"""GVC Resilience module.

Assesses supply chain resilience via three structural buffers:

1. **Trade openness** (NE.TRD.GNFS.ZS): (exports+imports)/GDP. Higher openness
   signals diversified trade relationships and the ability to re-source quickly.

2. **Import reserves** (FI.RES.TOTL.MO): total reserves in months of imports.
   More reserves = longer buffer to absorb supply disruptions without currency
   or payment crises.

3. **Manufacturing base** (NV.IND.MANF.ZS): manufacturing value added as % GDP.
   Domestic manufacturing capacity allows partial substitution when external
   supply chains break.

Scoring (each pillar 0-33.3 points, total 0-100):
  openness_score   = clip((50 - openness_pct) / 50 * 33.3, 0, 33.3)
                     -- low openness = lower diversification buffer
  reserves_score   = clip((6 - reserves_mo) / 6 * 33.3, 0, 33.3)
                     -- < 3 months = fragile; < 1 month = crisis
  manuf_score      = clip((15 - manuf_pct) / 15 * 33.3, 0, 33.3)
                     -- low domestic manufacturing = fragile position

Higher total score = lower GVC resilience = more stress.

Sources: World Bank WDI (NE.TRD.GNFS.ZS, FI.RES.TOTL.MO, NV.IND.MANF.ZS).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class GVCResilience(LayerBase):
    layer_id = "lVC"
    name = "GVC Resilience"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        openness_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.TRD.GNFS.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        reserves_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FI.RES.TOTL.MO'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        manuf_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NV.IND.MANF.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not openness_rows and not reserves_rows and not manuf_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no resilience data available"}

        # Openness pillar
        openness_pct = None
        openness_score = 16.65  # neutral fallback
        if openness_rows:
            openness_pct = float(np.mean([float(r["value"]) for r in openness_rows]))
            openness_score = float(np.clip((50.0 - openness_pct) / 50.0 * 33.3, 0.0, 33.3))

        # Reserves pillar
        reserves_mo = None
        reserves_score = 16.65  # neutral fallback
        if reserves_rows:
            reserves_mo = float(np.mean([float(r["value"]) for r in reserves_rows]))
            reserves_score = float(np.clip((6.0 - reserves_mo) / 6.0 * 33.3, 0.0, 33.3))

        # Manufacturing pillar
        manuf_pct = None
        manuf_score = 16.65  # neutral fallback
        if manuf_rows:
            manuf_pct = float(np.mean([float(r["value"]) for r in manuf_rows]))
            manuf_score = float(np.clip((15.0 - manuf_pct) / 15.0 * 33.3, 0.0, 33.3))

        score = float(np.clip(openness_score + reserves_score + manuf_score, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "trade_openness_pct_gdp": round(openness_pct, 2) if openness_pct is not None else None,
            "openness_score": round(openness_score, 1),
            "reserves_months_imports": round(reserves_mo, 2) if reserves_mo is not None else None,
            "reserves_score": round(reserves_score, 1),
            "manufacturing_pct_gdp": round(manuf_pct, 2) if manuf_pct is not None else None,
            "manuf_score": round(manuf_score, 1),
            "interpretation": (
                "fragile GVC position" if score > 65
                else "moderate GVC resilience" if score > 35
                else "resilient GVC position"
            ),
        }
