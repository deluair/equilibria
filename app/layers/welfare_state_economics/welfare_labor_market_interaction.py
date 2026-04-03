"""Welfare Labor Market Interaction module.

Examines how generous social transfers interact with unemployment.
High unemployment paired with low transfers signals inadequate labor
market support; the inverse may indicate benefit dependency risk.

Score reflects labor market vulnerability net of welfare cushion:
Score = clip(unemployment_pct * (1 - transfers_ratio) * 5, 0, 100)

Sources: WDI SL.UEM.TOTL.ZS (unemployment % total labor force),
         WDI GC.XPN.TRFT.ZS (social transfers % expense)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

TRANSFER_SCALE = 30.0


class WelfareLaborMarketInteraction(LayerBase):
    layer_id = "lWS"
    name = "Welfare Labor Market Interaction"

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
        unemployment = await self._fetch_mean(db, "SL.UEM.TOTL.ZS", "unemployment")
        transfers = await self._fetch_mean(db, "GC.XPN.TRFT.ZS", "social transfers")

        if unemployment is None and transfers is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no labor/transfers data"}

        unem = unemployment if unemployment is not None else 6.0
        trns = transfers if transfers is not None else 5.0

        transfers_ratio = min(1.0, trns / TRANSFER_SCALE)
        score = float(np.clip(unem * (1.0 - transfers_ratio) * 5, 0, 100))

        return {
            "score": round(score, 1),
            "unemployment_pct": round(unem, 2),
            "social_transfers_pct": round(trns, 2),
            "transfers_ratio": round(transfers_ratio, 3),
            "labor_vulnerability_net": round(unem * (1.0 - transfers_ratio), 2),
            "interpretation": (
                "high labor vulnerability, weak safety net" if score > 75
                else "elevated vulnerability" if score > 50
                else "moderate labor-welfare interaction" if score > 25
                else "low vulnerability, adequate safety net"
            ),
            "sources": ["WDI SL.UEM.TOTL.ZS", "WDI GC.XPN.TRFT.ZS"],
        }
