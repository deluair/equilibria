"""Air Connectivity Index module.

Measures air transport connectivity using air passengers carried (absolute
volume as proxy for network density and route coverage).

Indicator: IS.AIR.PSGR (air passengers carried).
Score reflects low connectivity when passenger volume is below a development
threshold. Inverted: low passengers = high score (worse connectivity).
Threshold: 50M passengers = frontier for mid-size economies.

Score = clip((1 - passengers / 50_000_000) * 100, 0, 100).

Sources: WDI IS.AIR.PSGR
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_FRONTIER_PASSENGERS = 50_000_000.0


class AirConnectivityIndex(LayerBase):
    layer_id = "lTR"
    name = "Air Connectivity Index"

    async def compute(self, db, **kwargs) -> dict:
        code = "IS.AIR.PSGR"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{code}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for IS.AIR.PSGR"}

        passengers = float(rows[0]["value"])
        score = float(np.clip((1.0 - passengers / _FRONTIER_PASSENGERS) * 100.0, 0, 100))

        all_vals = [float(r["value"]) for r in rows]
        trend = "growing" if len(all_vals) >= 3 and all_vals[0] > all_vals[-1] else "declining" if len(all_vals) >= 3 and all_vals[0] < all_vals[-1] else "stable"

        return {
            "score": round(score, 1),
            "signal": self.classify_signal(score),
            "metrics": {
                "air_passengers": int(passengers),
                "frontier_passengers": int(_FRONTIER_PASSENGERS),
                "coverage_ratio": round(passengers / _FRONTIER_PASSENGERS, 4),
                "passenger_trend": trend,
                "n_obs": len(all_vals),
            },
            "_sources": ["WDI:IS.AIR.PSGR"],
        }
