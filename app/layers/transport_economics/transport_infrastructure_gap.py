"""Transport Infrastructure Gap module.

Measures the gap between actual transport infrastructure quality and
best-practice frontier using paved roads share and LPI infrastructure score.

Indicators: IS.ROD.PAVE.ZS (paved roads % of total), LP.LPI.INFR.XQ (LPI infrastructure, 1-5).
Gap score = average of (100 - paved_roads_pct) and ((5 - lpi_infra) / 4 * 100).
Higher score = larger infrastructure gap = worse outcome.

Sources: WDI IS.ROD.PAVE.ZS, LP.LPI.INFR.XQ
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TransportInfrastructureGap(LayerBase):
    layer_id = "lTR"
    name = "Transport Infrastructure Gap"

    async def compute(self, db, **kwargs) -> dict:
        road_code = "IS.ROD.PAVE.ZS"
        lpi_code = "LP.LPI.INFR.XQ"

        road_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (road_code, f"%{road_code}%"),
        )
        lpi_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (lpi_code, f"%{lpi_code}%"),
        )

        if not road_rows and not lpi_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for IS.ROD.PAVE.ZS or LP.LPI.INFR.XQ"}

        components = []
        metrics: dict = {}

        if road_rows:
            paved_pct = float(road_rows[0]["value"])
            road_gap = float(np.clip(100.0 - paved_pct, 0, 100))
            components.append(road_gap)
            metrics["paved_roads_pct"] = round(paved_pct, 2)
            metrics["road_gap_score"] = round(road_gap, 2)

        if lpi_rows:
            lpi = float(lpi_rows[0]["value"])
            lpi_gap = float(np.clip((5.0 - lpi) / 4.0 * 100.0, 0, 100))
            components.append(lpi_gap)
            metrics["lpi_infrastructure"] = round(lpi, 3)
            metrics["lpi_gap_score"] = round(lpi_gap, 2)

        score = float(np.mean(components))

        return {
            "score": round(score, 1),
            "signal": self.classify_signal(score),
            "metrics": metrics,
            "_sources": ["WDI:IS.ROD.PAVE.ZS", "WDI:LP.LPI.INFR.XQ"],
        }
