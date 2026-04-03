"""Transport Quality Index module.

Composite measure of road, rail, and port quality using Logistics Performance
Index sub-scores and road infrastructure indicators.

Sources: WDI LP.LPI.INFR.XQ (LPI infrastructure quality, 1-5 scale).
Score = clip((5 - lpi_infra) / 4 * 100, 0, 100).
Score=0 means best-in-class; Score=100 means worst possible.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

LPI_MAX = 5.0
LPI_MIN = 1.0


class TransportQualityIndex(LayerBase):
    layer_id = "lIF"
    name = "Transport Quality Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        lpi_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'LP.LPI.INFR.XQ'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not lpi_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        lpi_infra = float(lpi_rows[0]["value"])
        score = float(np.clip((LPI_MAX - lpi_infra) / (LPI_MAX - LPI_MIN) * 100.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "lpi_infrastructure_score": round(lpi_infra, 3),
            "lpi_scale": "1 (worst) to 5 (best)",
            "reference_year": str(lpi_rows[0]["date"]),
            "interpretation": (
                "Poor transport infrastructure: significant constraint on trade and growth"
                if score > 60
                else "Below-average transport quality" if score > 40
                else "Moderate transport quality" if score > 20
                else "High-quality transport infrastructure"
            ),
            "_sources": ["WDI:LP.LPI.INFR.XQ"],
        }
