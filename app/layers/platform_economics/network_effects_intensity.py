"""Network Effects Intensity module.

Synergy between internet penetration and mobile subscription density.
High combined penetration amplifies network effects on platforms.

Score: higher network intensity = higher score (more platform network power).

Source: World Bank WDI (IT.NET.USER.ZS, IT.CEL.SETS.P2)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class NetworkEffectsIntensity(LayerBase):
    layer_id = "lPE"
    name = "Network Effects Intensity"

    async def compute(self, db, **kwargs) -> dict:
        code = "IT.NET.USER.ZS"
        name = "internet users"
        net_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        code2 = "IT.CEL.SETS.P2"
        name2 = "mobile cellular subscriptions"
        mob_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (code2, f"%{name2}%"),
        )

        if not net_rows and not mob_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no internet/mobile data"}

        net_vals = [float(r["value"]) for r in net_rows if r["value"] is not None]
        mob_vals = [float(r["value"]) for r in mob_rows if r["value"] is not None]

        net_mean = float(np.nanmean(net_vals)) if net_vals else None
        mob_mean = float(np.nanmean(mob_vals)) if mob_vals else None

        components, weights = [], []
        if net_mean is not None:
            components.append(float(np.clip(net_mean, 0, 100)))
            weights.append(0.5)
        if mob_mean is not None:
            # mobile subscriptions can exceed 100; cap at 150 then normalize
            components.append(float(np.clip(mob_mean / 1.5, 0, 100)))
            weights.append(0.5)

        if not components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable values"}

        total_w = sum(weights)
        score = sum(c * w for c, w in zip(components, weights)) / total_w
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 1),
            "internet_users_pct": round(net_mean, 2) if net_mean is not None else None,
            "mobile_subscriptions_per_100": round(mob_mean, 2) if mob_mean is not None else None,
            "_citation": "World Bank WDI: IT.NET.USER.ZS, IT.CEL.SETS.P2",
        }
