"""Universal Basic Income Readiness module.

Proxies a country's readiness to implement UBI by combining fiscal
capacity (tax revenue as % GDP) and automation vulnerability (manufacturing
share as proxy for routine task exposure).

High tax revenue provides the fiscal base; high manufacturing share
implies greater automation exposure and UBI justification.

Readiness score (higher = more ready but also more urgent):
Score = clip((tax_pct / 40 * 50) + (manf_pct / 30 * 50), 0, 100)
Inverted for stress signal: higher readiness pressure = higher score.

Sources: WDI GC.TAX.TOTL.GD.ZS (tax revenue % GDP),
         WDI NV.IND.MANF.ZS (manufacturing value added % GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class UniversalBasicIncomeReadiness(LayerBase):
    layer_id = "lWS"
    name = "Universal Basic Income Readiness"

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
        tax_rev = await self._fetch_mean(db, "GC.TAX.TOTL.GD.ZS", "tax revenue")
        manufacturing = await self._fetch_mean(db, "NV.IND.MANF.ZS", "manufacturing")

        if tax_rev is None and manufacturing is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no fiscal/automation proxy data"}

        tax = tax_rev if tax_rev is not None else 15.0
        manf = manufacturing if manufacturing is not None else 15.0

        # fiscal_score: how much fiscal room exists (40% GDP = strong capacity)
        fiscal_score = min(50.0, (tax / 40.0) * 50.0)
        # automation_pressure: higher manufacturing = more automation exposure
        automation_score = min(50.0, (manf / 30.0) * 50.0)

        # Combined: UBI urgency = fiscal capacity + automation pressure
        score = float(np.clip(fiscal_score + automation_score, 0, 100))

        return {
            "score": round(score, 1),
            "tax_revenue_pct_gdp": round(tax, 2),
            "manufacturing_pct_gdp": round(manf, 2),
            "fiscal_capacity_score": round(fiscal_score, 1),
            "automation_pressure_score": round(automation_score, 1),
            "interpretation": (
                "high UBI urgency and capacity" if score > 75
                else "moderate UBI consideration warranted" if score > 50
                else "limited UBI pressure" if score > 25
                else "low UBI urgency"
            ),
            "sources": ["WDI GC.TAX.TOTL.GD.ZS", "WDI NV.IND.MANF.ZS"],
        }
