"""Connectivity Index module.

Composite multimodal connectivity score combining logistics performance,
internet access, and air transport capacity.

Sources: WDI LP.LPI.OVRL.XQ (Logistics Performance Index, overall, 1-5),
         WDI IT.NET.USER.ZS (internet users, % of population),
         WDI IS.AIR.PSGR (air transport, passengers carried).
Score = clip(100 - composite_connectivity * 20, 0, 100).
Each sub-index normalized 0-100; composite is equal-weighted average.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

LPI_MAX = 5.0
LPI_MIN = 1.0


class ConnectivityIndex(LayerBase):
    layer_id = "lIF"
    name = "Connectivity Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        lpi_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'LP.LPI.OVRL.XQ'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        inet_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IT.NET.USER.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        air_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IS.AIR.PSGR'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        components = {}

        if lpi_rows:
            lpi = float(lpi_rows[0]["value"])
            components["logistics"] = (lpi - LPI_MIN) / (LPI_MAX - LPI_MIN) * 100.0

        if inet_rows:
            components["digital"] = float(inet_rows[0]["value"])

        if air_rows:
            # Air passengers: normalize using log scale; 100M pax ~ 100 score
            pax = float(air_rows[0]["value"])
            components["air"] = float(np.clip(np.log1p(pax) / np.log1p(1e8) * 100.0, 0, 100))

        if not components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        composite_connectivity = float(np.mean(list(components.values())))
        # High connectivity -> low stress
        score = float(np.clip(100.0 - composite_connectivity, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "composite_connectivity_index": round(composite_connectivity, 2),
            "sub_indices": {k: round(v, 2) for k, v in components.items()},
            "lpi_overall": round(float(lpi_rows[0]["value"]), 3) if lpi_rows else None,
            "internet_users_pct": round(float(inet_rows[0]["value"]), 2) if inet_rows else None,
            "air_passengers": int(float(air_rows[0]["value"])) if air_rows else None,
            "interpretation": (
                "Poor multimodal connectivity: significant constraint on commerce and mobility"
                if score > 60
                else "Below-average connectivity" if score > 40
                else "Moderate connectivity" if score > 20
                else "High multimodal connectivity"
            ),
            "_sources": ["WDI:LP.LPI.OVRL.XQ", "WDI:IT.NET.USER.ZS", "WDI:IS.AIR.PSGR"],
        }
