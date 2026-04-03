"""Winner Takes All Dynamics module.

High income inequality combined with high digital concentration amplifies
winner-takes-all platform outcomes.

Score: higher dynamics intensity = higher score (worse).

Source: World Bank WDI (SI.POV.GINI, IT.NET.USER.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class WinnerTakesAllDynamics(LayerBase):
    layer_id = "lPE"
    name = "Winner Takes All Dynamics"

    async def compute(self, db, **kwargs) -> dict:
        code = "SI.POV.GINI"
        name = "gini"
        gini_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        code2 = "IT.NET.USER.ZS"
        name2 = "internet users"
        net_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (code2, f"%{name2}%"),
        )

        if not gini_rows and not net_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no GINI/internet data"}

        gini_vals = [float(r["value"]) for r in gini_rows if r["value"] is not None]
        net_vals = [float(r["value"]) for r in net_rows if r["value"] is not None]

        gini_mean = float(np.nanmean(gini_vals)) if gini_vals else None
        net_mean = float(np.nanmean(net_vals)) if net_vals else None

        components, weights = [], []
        if gini_mean is not None:
            # GINI 0-100; higher = more inequality = more winner-takes-all risk
            components.append(float(np.clip(gini_mean, 0, 100)))
            weights.append(0.5)
        if net_mean is not None:
            # Higher internet penetration amplifies winner-takes-all concentration
            components.append(float(np.clip(net_mean, 0, 100)))
            weights.append(0.5)

        if not components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable values"}

        total_w = sum(weights)
        score = sum(c * w for c, w in zip(components, weights)) / total_w
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 1),
            "gini_index": round(gini_mean, 2) if gini_mean is not None else None,
            "internet_users_pct": round(net_mean, 2) if net_mean is not None else None,
            "_citation": "World Bank WDI: SI.POV.GINI, IT.NET.USER.ZS",
        }
