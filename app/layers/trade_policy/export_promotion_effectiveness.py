"""Export Promotion Effectiveness module.

Compares export volume growth against GDP growth. When exports consistently
lag behind overall GDP growth, export promotion policy is failing to leverage
the economy's productive capacity in international markets.

Score = clip(max(0, gdp_growth - export_growth) * 5, 0, 100)

Sources: WDI
  NE.EXP.GNFS.KD.ZG - Exports of goods and services (annual % growth)
  NY.GDP.MKTP.KD.ZG  - GDP growth (annual %)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ExportPromotionEffectiveness(LayerBase):
    layer_id = "lTP"
    name = "Export Promotion Effectiveness"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        export_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.EXP.GNFS.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        gdp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not export_rows or not gdp_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient export or GDP growth data",
            }

        export_vals = [float(r["value"]) for r in export_rows if r["value"] is not None]
        gdp_vals = [float(r["value"]) for r in gdp_rows if r["value"] is not None]

        if len(export_vals) < 3 or len(gdp_vals) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "too few valid observations"}

        export_growth = float(np.mean(export_vals[-5:]))
        gdp_growth = float(np.mean(gdp_vals[-5:]))

        gap = gdp_growth - export_growth  # positive = exports lagging
        score = float(np.clip(max(0, gap) * 5, 0, 100))

        effectiveness = (
            "strong" if export_growth >= gdp_growth + 1
            else "adequate" if export_growth >= gdp_growth - 1
            else "weak" if gap < 5
            else "failing"
        )

        return {
            "score": round(score, 1),
            "country": country,
            "mean_export_growth_pct": round(export_growth, 2),
            "mean_gdp_growth_pct": round(gdp_growth, 2),
            "export_gdp_growth_gap": round(gap, 2),
            "promotion_effectiveness": effectiveness,
            "n_export_obs": len(export_vals),
            "n_gdp_obs": len(gdp_vals),
        }
