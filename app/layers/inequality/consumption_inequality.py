"""Consumption Inequality module.

Measures household consumption distribution, focusing on the bottom quintile's
share of total income/consumption.

A low bottom-quintile share (income share of lowest 20%) signals that consumption
is concentrated at the top, leaving the poorest households with insufficient
purchasing power.

Primary indicator:
- SI.DST.FRST.20: Income share held by lowest 20% (WDI)

Fallback (when SI.DST.FRST.20 unavailable):
- SI.POV.GINI: Gini proxy. Low Gini ~ higher bottom share.
  bottom_20_approx = 10 - (gini - 30) * 0.2 (rough approximation)

Score = clip((10 - bottom_20_share) * 10, 0, 100)
  - bottom_20 = 10% (typical): score = 0
  - bottom_20 = 5%: score = 50
  - bottom_20 = 2%: score = 80

Sources: World Bank WDI (SI.DST.FRST.20, SI.POV.GINI)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ConsumptionInequality(LayerBase):
    layer_id = "lIQ"
    name = "Consumption Inequality"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        bottom_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.DST.FRST.20'
            ORDER BY dp.date DESC
            LIMIT 5
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
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if bottom_rows:
            bottom_20 = float(bottom_rows[0]["value"])
            source = "observed_SI.DST.FRST.20"
            gini = float(gini_rows[0]["value"]) if gini_rows else None
        elif gini_rows:
            gini = float(gini_rows[0]["value"])
            # Approximate bottom 20% share from Gini (rough linear proxy)
            bottom_20 = float(np.clip(10.0 - (gini - 30.0) * 0.2, 1.0, 15.0))
            source = "approximated_from_gini"
        else:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        # Score: deviation below a 10% benchmark
        score = float(np.clip((10.0 - bottom_20) * 10.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "bottom_20_income_share_pct": round(bottom_20, 2),
            "gini": round(gini, 2) if gini is not None else None,
            "source": source,
            "interpretation": {
                "very_low_bottom_share": bottom_20 < 4,
                "low_bottom_share": bottom_20 < 7,
                "benchmark_pct": 10.0,
            },
        }
