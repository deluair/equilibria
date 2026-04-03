"""Poverty Severity Index module.

Computes the squared poverty gap (SI.POV.SQGP) at the $2.15/day line. By
squaring individual shortfalls before averaging, this measure gives greater
weight to the poorest of the poor and is sensitive to inequality among the
poor (Foster-Greer-Thorbecke P2).

Score = clip(sqgap * 25, 0, 100).

Sources: WDI (SI.POV.SQGP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PovertyGapSqIndex(LayerBase):
    layer_id = "lPM"
    name = "Poverty Severity Index"

    async def compute(self, db, **kwargs) -> dict:
        code = "SI.POV.SQGP"
        name = "squared poverty gap"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for SI.POV.SQGP"}

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid values"}

        latest = values[0]
        mean_val = float(np.mean(values))
        trend = float(np.polyfit(range(len(values)), values, 1)[0]) if len(values) >= 3 else 0.0

        score = float(np.clip(latest * 25, 0, 100))

        return {
            "score": round(score, 1),
            "squared_gap": round(latest, 4),
            "mean_squared_gap": round(mean_val, 4),
            "trend_per_period": round(trend, 5),
            "n_obs": len(values),
            "poverty_line": "$2.15/day (2017 PPP)",
            "indicator": code,
            "fgt_class": "P2 (Foster-Greer-Thorbecke)",
            "interpretation": "higher weight to poorest; sensitive to intra-poor inequality",
        }
