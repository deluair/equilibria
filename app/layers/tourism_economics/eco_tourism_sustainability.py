"""Eco-Tourism Sustainability module.

Proxies eco-tourism asset quality via forest cover (AG.LND.FRST.ZS — World Bank WDI).
High and stable forest cover supports nature-based tourism. Declining cover signals
degradation of eco-tourism resources.

Score: 0 (excellent, high stable forest cover) to 100 (critical: low/declining cover).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class EcoTourismSustainability(LayerBase):
    layer_id = "lTO"
    name = "Eco-Tourism Sustainability"

    async def compute(self, db, **kwargs) -> dict:
        code = "AG.LND.FRST.ZS"
        name = "forest area"

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
                "error": "no data for AG.LND.FRST.ZS (forest cover % land area)",
            }

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "all values null for forest cover",
            }

        latest = values[0]
        arr = np.array(values[::-1])  # chronological

        # Trend: declining forest = rising score
        trend_slope = 0.0
        if len(arr) >= 3:
            t = np.arange(len(arr), dtype=float)
            slope, _ = np.polyfit(t, arr, 1)
            trend_slope = float(slope)

        # Score components:
        # cover_score: 0% cover -> 100, 50% -> 50, 100% -> 0
        cover_score = float(np.clip(100 - latest * 1.0, 0, 100))
        # trend_penalty: declining slope adds up to 20 pts
        trend_penalty = float(np.clip(-trend_slope * 5, 0, 20))
        score = float(np.clip(cover_score * 0.8 + trend_penalty, 0, 100))

        return {
            "score": round(score, 1),
            "indicator": code,
            "forest_cover_pct": round(latest, 2),
            "trend_slope_per_period": round(trend_slope, 4),
            "cover_score": round(cover_score, 1),
            "trend_penalty": round(trend_penalty, 1),
            "n_obs": len(values),
            "methodology": "score = clip(cover_score * 0.8 + trend_penalty, 0, 100)",
        }
