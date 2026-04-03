"""Welfare Fiscal Sustainability module.

Assesses whether social spending is compatible with fiscal balance.

High social transfers combined with a large fiscal deficit signal
sustainability risk. Score rises with the mismatch between generosity
and fiscal headroom.

Score = clip(max(0, transfers_pct - fiscal_balance_adj) * 5, 0, 100)
where fiscal_balance_adj normalizes the fiscal balance to a 0-20 scale.

Sources: WDI GC.XPN.TRFT.ZS (social transfers % expense),
         WDI GC.BAL.CASH.GD.ZS (fiscal balance % GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class WelfareFiscalSustainability(LayerBase):
    layer_id = "lWS"
    name = "Welfare Fiscal Sustainability"

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
        fiscal_balance = await self._fetch_mean(db, "GC.BAL.CASH.GD.ZS", "fiscal balance")

        if transfers is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no social transfers data"}

        # fiscal_balance is negative when in deficit; convert to a headroom metric
        # headroom: 0 = balanced budget, positive surplus reduces risk
        headroom = fiscal_balance if fiscal_balance is not None else -3.0  # assume modest deficit

        # sustainability pressure: high transfers + large deficit
        # normalize: transfers benchmarked at 20%, headroom penalty up to 10 pct points
        pressure = transfers - max(0.0, 10.0 + headroom)
        score = float(np.clip(max(0.0, pressure) * 5, 0, 100))

        return {
            "score": round(score, 1),
            "social_transfers_pct": round(transfers, 2),
            "fiscal_balance_pct_gdp": round(fiscal_balance, 2) if fiscal_balance is not None else None,
            "sustainability_pressure": round(pressure, 2),
            "interpretation": (
                "critical sustainability risk" if score > 75
                else "elevated risk" if score > 50
                else "moderate risk" if score > 25
                else "fiscally sustainable"
            ),
            "sources": ["WDI GC.XPN.TRFT.ZS", "WDI GC.BAL.CASH.GD.ZS"],
        }
