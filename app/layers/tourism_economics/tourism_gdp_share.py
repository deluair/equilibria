"""Tourism GDP Share module.

Measures international tourism receipts as a share of total exports
(ST.INT.RCPT.XP.ZS — World Bank WDI). Higher share indicates greater
economic contribution from tourism but also higher dependence risk.

Score: 100 = negligible tourism contribution (<1%); 0 = very high (>50%).
Moderate tourism integration (5-20%) scores around 50.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TourismGdpShare(LayerBase):
    layer_id = "lTO"
    name = "Tourism GDP Share"

    async def compute(self, db, **kwargs) -> dict:
        code = "ST.INT.RCPT.XP.ZS"
        name = "international tourism receipts"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for ST.INT.RCPT.XP.ZS (tourism receipts % exports)",
            }

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "all values null for tourism GDP share",
            }

        latest = values[0]
        avg = float(np.mean(values))

        # Score: low share = low economic contribution = higher score (risk of underdevelopment)
        # A very low share means tourism isn't contributing meaningfully
        # Map: 0% -> 80, 10% -> 40, 30% -> 20, >50% -> 5
        score = float(np.clip(80 - latest * 1.5, 5, 95))

        return {
            "score": round(score, 1),
            "indicator": code,
            "latest_pct": round(latest, 2),
            "mean_pct": round(avg, 2),
            "n_obs": len(values),
            "methodology": "score = clip(80 - share * 1.5, 5, 95); low share = lower tourism contribution",
        }
