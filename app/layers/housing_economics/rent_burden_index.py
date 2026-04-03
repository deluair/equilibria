"""Rent Burden Index module.

Proxies the share of households paying more than 30% of income on rent
using urban population share (SP.URB.TOTL.IN.ZS), Gini coefficient
(SI.POV.GINI), and CPI inflation (FP.CPI.TOTL.ZG). High urbanisation,
high inequality, and persistent inflation compound rent burden risk.

Score = clip((urban_pct * 0.4) + (gini * 0.4) + (inflation * 0.5) - 30, 0, 100)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RentBurdenIndex(LayerBase):
    layer_id = "lHO"
    name = "Rent Burden Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        urban_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.URB.TOTL.IN.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        gini_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.POV.GINI'
            ORDER BY dp.date
            """,
            (country,),
        )

        cpi_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FP.CPI.TOTL.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not urban_rows or len(urban_rows) < 1:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient urban population data for rent burden index",
            }

        if not gini_rows or len(gini_rows) < 1:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient Gini data for rent burden index",
            }

        if not cpi_rows or len(cpi_rows) < 1:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient CPI data for rent burden index",
            }

        urban_pct = float(np.array([float(r["value"]) for r in urban_rows])[-1])
        gini = float(np.array([float(r["value"]) for r in gini_rows])[-1])
        inflation = float(np.mean([float(r["value"]) for r in cpi_rows[-3:]]))

        raw_score = (urban_pct * 0.4) + (gini * 0.4) + (inflation * 0.5) - 30
        score = float(np.clip(raw_score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "urban_population_pct": round(urban_pct, 2),
            "gini_index": round(gini, 2),
            "avg_cpi_inflation_pct": round(inflation, 2),
            "burden_composite": round((urban_pct * 0.4) + (gini * 0.4) + (inflation * 0.5), 2),
            "n_urban_obs": len(urban_rows),
            "n_gini_obs": len(gini_rows),
            "n_cpi_obs": len(cpi_rows),
            "methodology": "score = clip((urban_pct*0.4) + (gini*0.4) + (inflation*0.5) - 30, 0, 100)",
        }
