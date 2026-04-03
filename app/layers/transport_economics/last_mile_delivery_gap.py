"""Last Mile Delivery Gap module.

Estimates last-mile delivery difficulty by combining rural population share
with paved road coverage. High rural population share + low paved roads =
severe last-mile connectivity gap, raising logistics costs for remote areas.

Indicators: SP.RUR.TOTL.ZS (rural population % of total),
            IS.ROD.PAVE.ZS (paved roads % of total roads).
Gap = rural_pct * (1 - paved_pct / 100).
Score = clip(gap, 0, 100). Higher = worse last-mile connectivity.

Sources: WDI SP.RUR.TOTL.ZS, IS.ROD.PAVE.ZS
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class LastMileDeliveryGap(LayerBase):
    layer_id = "lTR"
    name = "Last Mile Delivery Gap"

    async def compute(self, db, **kwargs) -> dict:
        rur_code = "SP.RUR.TOTL.ZS"
        road_code = "IS.ROD.PAVE.ZS"

        rur_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (rur_code, f"%{rur_code}%"),
        )
        road_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (road_code, f"%{road_code}%"),
        )

        if not rur_rows and not road_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for SP.RUR.TOTL.ZS or IS.ROD.PAVE.ZS"}

        metrics: dict = {}

        rural_pct = float(rur_rows[0]["value"]) if rur_rows else 50.0
        paved_pct = float(road_rows[0]["value"]) if road_rows else 50.0

        if rur_rows:
            metrics["rural_population_pct"] = round(rural_pct, 2)
        if road_rows:
            metrics["paved_roads_pct"] = round(paved_pct, 2)

        # Gap = fraction of rural population without paved road access
        gap = rural_pct * (1.0 - paved_pct / 100.0)
        score = float(np.clip(gap, 0, 100))
        metrics["last_mile_gap_index"] = round(gap, 3)
        metrics["data_available"] = {"rural": bool(rur_rows), "roads": bool(road_rows)}

        return {
            "score": round(score, 1),
            "signal": self.classify_signal(score),
            "metrics": metrics,
            "_sources": ["WDI:SP.RUR.TOTL.ZS", "WDI:IS.ROD.PAVE.ZS"],
        }
