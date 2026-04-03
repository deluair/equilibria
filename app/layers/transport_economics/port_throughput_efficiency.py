"""Port Throughput Efficiency module.

Measures port throughput capacity and tracking/tracing capability as proxies
for port efficiency. Combines container port throughput (TEU) with LPI
tracking and tracing sub-score.

Indicators: IS.SHP.GOOD.TU (container port traffic, TEU thousands),
            LP.LPI.TRAC.XQ (LPI tracking and tracing, 1-5 scale).
Score = average of throughput gap and tracking gap components.
Higher score = lower port efficiency = worse outcome.

Sources: WDI IS.SHP.GOOD.TU, LP.LPI.TRAC.XQ
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_FRONTIER_TEU = 50_000.0  # 50M TEU = frontier for major economies (in thousands)


class PortThroughputEfficiency(LayerBase):
    layer_id = "lTR"
    name = "Port Throughput Efficiency"

    async def compute(self, db, **kwargs) -> dict:
        teu_code = "IS.SHP.GOOD.TU"
        lpi_code = "LP.LPI.TRAC.XQ"

        teu_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (teu_code, f"%{teu_code}%"),
        )
        lpi_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (lpi_code, f"%{lpi_code}%"),
        )

        if not teu_rows and not lpi_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for IS.SHP.GOOD.TU or LP.LPI.TRAC.XQ"}

        components = []
        metrics: dict = {}

        if teu_rows:
            teu = float(teu_rows[0]["value"])
            teu_gap = float(np.clip((1.0 - teu / _FRONTIER_TEU) * 100.0, 0, 100))
            components.append(teu_gap)
            metrics["container_port_teu_thousands"] = round(teu, 1)
            metrics["teu_gap_score"] = round(teu_gap, 2)

        if lpi_rows:
            lpi_trac = float(lpi_rows[0]["value"])
            trac_gap = float(np.clip((5.0 - lpi_trac) / 4.0 * 100.0, 0, 100))
            components.append(trac_gap)
            metrics["lpi_tracking"] = round(lpi_trac, 3)
            metrics["tracking_gap_score"] = round(trac_gap, 2)

        score = float(np.mean(components))

        return {
            "score": round(score, 1),
            "signal": self.classify_signal(score),
            "metrics": metrics,
            "_sources": ["WDI:IS.SHP.GOOD.TU", "WDI:LP.LPI.TRAC.XQ"],
        }
