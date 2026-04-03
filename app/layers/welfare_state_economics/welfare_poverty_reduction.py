"""Welfare Poverty Reduction module.

Estimates welfare state effectiveness in reducing poverty by combining
poverty headcount and social transfer generosity as a proxy for before/after
transfer impact.

High poverty alongside low transfers = poor redistribution effectiveness.
Score = clip((poverty_rate * (1 - transfers_ratio)) * 5, 0, 100)

Sources: WDI SI.POV.DDAY (poverty headcount ratio at $2.15/day, % population),
         WDI GC.XPN.TRFT.ZS (social transfers as % of expense)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

TRANSFER_SCALE = 30.0  # roughly the max plausible transfers % expense


class WelfarePovertyReduction(LayerBase):
    layer_id = "lWS"
    name = "Welfare Poverty Reduction"

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
        poverty = await self._fetch_mean(db, "SI.POV.DDAY", "poverty headcount")
        transfers = await self._fetch_mean(db, "GC.XPN.TRFT.ZS", "social transfers")

        if poverty is None and transfers is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no poverty or transfers data"}

        pov = poverty if poverty is not None else 10.0  # fallback
        trns = transfers if transfers is not None else 5.0  # fallback: assume minimal

        # transfers_ratio: 0 (no redistribution) to 1 (full benchmark)
        transfers_ratio = min(1.0, trns / TRANSFER_SCALE)
        score = float(np.clip(pov * (1.0 - transfers_ratio) * 5, 0, 100))

        return {
            "score": round(score, 1),
            "poverty_headcount_pct": round(pov, 2),
            "social_transfers_pct": round(trns, 2),
            "transfers_ratio": round(transfers_ratio, 3),
            "interpretation": (
                "very weak poverty reduction" if score > 75
                else "weak poverty reduction" if score > 50
                else "moderate poverty reduction" if score > 25
                else "strong poverty reduction"
            ),
            "sources": ["WDI SI.POV.DDAY", "WDI GC.XPN.TRFT.ZS"],
        }
