"""Welfare Inequality Nexus module.

Examines whether welfare spending translates into lower inequality.
Combines the Gini coefficient with social transfer generosity to
assess redistribution effectiveness.

High Gini + low transfers = poor redistribution (high score).
Low Gini + high transfers = effective welfare state (low score).

Score = clip((gini * (1 - transfers_ratio)) * 1.5, 0, 100)

Sources: WDI SI.POV.GINI (Gini index, 0-100 scale),
         WDI GC.XPN.TRFT.ZS (social transfers % expense)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

TRANSFER_SCALE = 30.0  # roughly the max plausible transfers % expense


class WelfareInequalityNexus(LayerBase):
    layer_id = "lWS"
    name = "Welfare Inequality Nexus"

    async def _fetch_mean(self, db, code: str, name: str) -> float | None:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        vals = [float(r["value"]) for r in rows if r["value"] is not None]
        return float(np.mean(vals)) if vals else None

    async def compute(self, db, **kwargs) -> dict:
        gini = await self._fetch_mean(db, "SI.POV.GINI", "Gini")
        transfers = await self._fetch_mean(db, "GC.XPN.TRFT.ZS", "social transfers")

        if gini is None and transfers is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no inequality or transfers data"}

        g = gini if gini is not None else 40.0  # global average approx
        trns = transfers if transfers is not None else 5.0

        transfers_ratio = min(1.0, trns / TRANSFER_SCALE)
        score = float(np.clip(g * (1.0 - transfers_ratio) * 1.5, 0, 100))

        return {
            "score": round(score, 1),
            "gini_index": round(g, 2),
            "social_transfers_pct": round(trns, 2),
            "transfers_ratio": round(transfers_ratio, 3),
            "redistribution_gap": round(g * (1.0 - transfers_ratio), 2),
            "interpretation": (
                "high inequality, weak redistribution" if score > 75
                else "elevated inequality-redistribution gap" if score > 50
                else "moderate redistribution effectiveness" if score > 25
                else "effective inequality reduction"
            ),
            "sources": ["WDI SI.POV.GINI", "WDI GC.XPN.TRFT.ZS"],
        }
