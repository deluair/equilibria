"""Platform Market Dominance module.

Market concentration in services sector as a proxy for platform dominance.
Higher services share + easier business environment = more platform-favorable conditions.

Score: higher dominance risk = higher score (worse).

Source: World Bank WDI (NV.SRV.TOTL.ZS, IC.BUS.EASE.XQ)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PlatformMarketDominance(LayerBase):
    layer_id = "lPE"
    name = "Platform Market Dominance"

    async def compute(self, db, **kwargs) -> dict:
        code = "NV.SRV.TOTL.ZS"
        name = "services value added"
        srv_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        code2 = "IC.BUS.EASE.XQ"
        name2 = "ease of doing business"
        ease_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (code2, f"%{name2}%"),
        )

        if not srv_rows and not ease_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no services/business ease data"}

        srv_vals = [float(r["value"]) for r in srv_rows if r["value"] is not None]
        ease_vals = [float(r["value"]) for r in ease_rows if r["value"] is not None]

        srv_mean = float(np.nanmean(srv_vals)) if srv_vals else None
        ease_mean = float(np.nanmean(ease_vals)) if ease_vals else None

        components, weights = [], []
        if srv_mean is not None:
            components.append(float(np.clip(srv_mean, 0, 100)))
            weights.append(0.6)
        if ease_mean is not None:
            components.append(float(np.clip(ease_mean, 0, 100)))
            weights.append(0.4)

        if not components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable values"}

        total_w = sum(weights)
        score = sum(c * w for c, w in zip(components, weights)) / total_w
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 1),
            "services_value_added_pct": round(srv_mean, 2) if srv_mean is not None else None,
            "ease_of_business_score": round(ease_mean, 2) if ease_mean is not None else None,
            "_citation": "World Bank WDI: NV.SRV.TOTL.ZS, IC.BUS.EASE.XQ",
        }
