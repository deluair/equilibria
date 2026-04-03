"""Visa Openness Index module.

Proxies trade and travel openness via trade in goods and services as % of GDP
(NE.TRD.GNFS.ZS — World Bank WDI). Higher trade openness correlates with
more liberalised entry policies, including tourism visas and travel facilitation.

Score: 0 (very open) to 100 (very closed/restricted).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class VisaOpennessIndex(LayerBase):
    layer_id = "lTO"
    name = "Visa Openness Index"

    async def compute(self, db, **kwargs) -> dict:
        code = "NE.TRD.GNFS.ZS"
        name = "trade in goods and services"

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
                "error": "no data for NE.TRD.GNFS.ZS (trade openness % GDP)",
            }

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "all values null for trade openness",
            }

        latest = values[0]
        avg = float(np.mean(values))

        # Higher trade openness = more open = lower score
        # Map: 0% -> 100, 50% -> 60, 100% -> 40, 200% -> 10
        score = float(np.clip(100 - latest * 0.45, 5, 100))

        return {
            "score": round(score, 1),
            "indicator": code,
            "trade_openness_pct_gdp": round(latest, 2),
            "mean_openness_pct_gdp": round(avg, 2),
            "n_obs": len(values),
            "methodology": "score = clip(100 - openness * 0.45, 5, 100); proxy for visa/travel openness",
        }
