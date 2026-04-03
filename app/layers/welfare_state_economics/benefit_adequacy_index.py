"""Benefit Adequacy Index module.

Measures whether social transfers are adequate relative to the depth
of poverty. If the poverty gap is wide and transfers are small,
benefits are inadequate.

Score = clip((poverty_gap / max(transfers_pct, 0.1)) * 20, 0, 100)

Sources: WDI GC.XPN.TRFT.ZS (social transfers % expense),
         WDI SI.POV.GAPS (poverty gap at $2.15/day, % poverty line)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class BenefitAdequacyIndex(LayerBase):
    layer_id = "lWS"
    name = "Benefit Adequacy Index"

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
        transfers = await self._fetch_mean(db, "GC.XPN.TRFT.ZS", "social transfers")
        poverty_gap = await self._fetch_mean(db, "SI.POV.GAPS", "poverty gap")

        if transfers is None and poverty_gap is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no benefit adequacy data"}

        trns = transfers if transfers is not None else 5.0
        gap = poverty_gap if poverty_gap is not None else 5.0

        # Adequacy ratio: higher transfers relative to gap = more adequate
        score = float(np.clip((gap / max(trns, 0.1)) * 20, 0, 100))

        return {
            "score": round(score, 1),
            "social_transfers_pct": round(trns, 2),
            "poverty_gap_pct": round(gap, 2),
            "adequacy_ratio": round(gap / max(trns, 0.1), 3),
            "interpretation": (
                "severely inadequate benefits" if score > 75
                else "inadequate benefits" if score > 50
                else "partially adequate benefits" if score > 25
                else "adequate benefits"
            ),
            "sources": ["WDI GC.XPN.TRFT.ZS", "WDI SI.POV.GAPS"],
        }
