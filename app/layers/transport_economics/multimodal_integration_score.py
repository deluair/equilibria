"""Multimodal Integration Score module.

Composite score measuring the degree to which road, air, and logistics
networks are jointly developed. Gaps across modes indicate fragmented
transport systems with higher transfer costs and last-mile inefficiencies.

Indicators:
  IS.ROD.PAVE.ZS  (road — paved roads %)
  IS.AIR.PSGR     (air — passengers as connectivity proxy)
  LP.LPI.OVRL.XQ  (logistics — LPI overall, 1-5)

Score = average gap across three modal components. Higher = more fragmented.

Sources: WDI IS.ROD.PAVE.ZS, IS.AIR.PSGR, LP.LPI.OVRL.XQ
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_AIR_FRONTIER = 50_000_000.0


class MultimodalIntegrationScore(LayerBase):
    layer_id = "lTR"
    name = "Multimodal Integration Score"

    async def compute(self, db, **kwargs) -> dict:
        road_code = "IS.ROD.PAVE.ZS"
        air_code = "IS.AIR.PSGR"
        lpi_code = "LP.LPI.OVRL.XQ"

        road_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (road_code, f"%{road_code}%"),
        )
        air_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (air_code, f"%{air_code}%"),
        )
        lpi_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (lpi_code, f"%{lpi_code}%"),
        )

        if not road_rows and not air_rows and not lpi_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for road, air, or logistics indicators"}

        components = []
        metrics: dict = {}

        if road_rows:
            paved = float(road_rows[0]["value"])
            road_gap = float(np.clip(100.0 - paved, 0, 100))
            components.append(road_gap)
            metrics["paved_roads_pct"] = round(paved, 2)
            metrics["road_modal_gap"] = round(road_gap, 2)

        if air_rows:
            passengers = float(air_rows[0]["value"])
            air_gap = float(np.clip((1.0 - passengers / _AIR_FRONTIER) * 100.0, 0, 100))
            components.append(air_gap)
            metrics["air_passengers"] = int(passengers)
            metrics["air_modal_gap"] = round(air_gap, 2)

        if lpi_rows:
            lpi = float(lpi_rows[0]["value"])
            lpi_gap = float(np.clip((5.0 - lpi) / 4.0 * 100.0, 0, 100))
            components.append(lpi_gap)
            metrics["lpi_overall"] = round(lpi, 3)
            metrics["logistics_modal_gap"] = round(lpi_gap, 2)

        score = float(np.mean(components))

        return {
            "score": round(score, 1),
            "signal": self.classify_signal(score),
            "metrics": metrics,
            "_sources": ["WDI:IS.ROD.PAVE.ZS", "WDI:IS.AIR.PSGR", "WDI:LP.LPI.OVRL.XQ"],
        }
