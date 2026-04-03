"""Road Quality Index module.

Measures road network quality via the share of paved roads in total road
network. Low paved road share = poor connectivity, higher transport costs,
limited access for rural populations and freight.

Indicator: IS.ROD.PAVE.ZS (paved roads as % of total roads).
Score = clip(100 - paved_pct, 0, 100). Higher score = worse road quality.

Sources: WDI IS.ROD.PAVE.ZS
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RoadQualityIndex(LayerBase):
    layer_id = "lTR"
    name = "Road Quality Index"

    async def compute(self, db, **kwargs) -> dict:
        code = "IS.ROD.PAVE.ZS"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{code}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for IS.ROD.PAVE.ZS"}

        paved_pct = float(rows[0]["value"])
        score = float(np.clip(100.0 - paved_pct, 0, 100))

        all_vals = [float(r["value"]) for r in rows]
        trend = "improving" if len(all_vals) >= 3 and all_vals[0] > all_vals[-1] else "deteriorating" if len(all_vals) >= 3 and all_vals[0] < all_vals[-1] else "stable"

        return {
            "score": round(score, 1),
            "signal": self.classify_signal(score),
            "metrics": {
                "paved_roads_pct": round(paved_pct, 2),
                "unpaved_roads_pct": round(100.0 - paved_pct, 2),
                "road_quality_trend": trend,
                "n_obs": len(all_vals),
            },
            "_sources": ["WDI:IS.ROD.PAVE.ZS"],
        }
