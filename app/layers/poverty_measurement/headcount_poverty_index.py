"""Headcount Poverty Index module.

Measures the share of the population living below the international poverty
line of $2.15/day (2017 PPP), the World Bank's extreme poverty threshold
(SI.POV.DDAY). Higher headcount = more stress.

Score = clip(headcount_pct * 2, 0, 100).

Sources: WDI (SI.POV.DDAY)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HeadcountPovertyIndex(LayerBase):
    layer_id = "lPM"
    name = "Headcount Poverty Index"

    async def compute(self, db, **kwargs) -> dict:
        code = "SI.POV.DDAY"
        name = "poverty headcount"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for SI.POV.DDAY"}

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid values"}

        latest = values[0]
        mean_val = float(np.mean(values))
        trend = float(np.polyfit(range(len(values)), values, 1)[0]) if len(values) >= 3 else 0.0

        score = float(np.clip(latest * 2, 0, 100))

        return {
            "score": round(score, 1),
            "headcount_pct": round(latest, 3),
            "mean_pct": round(mean_val, 3),
            "trend_per_period": round(trend, 4),
            "n_obs": len(values),
            "poverty_line": "$2.15/day (2017 PPP)",
            "indicator": code,
        }
