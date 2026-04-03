"""Universalism Index module.

Measures coverage breadth of the welfare state by combining health,
education, and social transfer spending as % of GDP.

A universalist welfare state delivers broad-based services. The index
averages normalized coverage of three pillars: health (SH.XPD.CHEX.GD.ZS),
education (SE.XPD.TOTL.GD.ZS), and social transfers (GC.XPN.TRFT.ZS).

Score = 100 - clip(combined_coverage / 3, 0, 100)
(higher score = narrower / less universal coverage)

Sources: WDI SH.XPD.CHEX.GD.ZS, SE.XPD.TOTL.GD.ZS, GC.XPN.TRFT.ZS
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# Expected high-coverage benchmark per pillar (% GDP)
HEALTH_BENCH = 8.0
EDU_BENCH = 6.0
TRANSFER_BENCH = 15.0


class UniversalismIndex(LayerBase):
    layer_id = "lWS"
    name = "Universalism Index"

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
        health = await self._fetch_mean(db, "SH.XPD.CHEX.GD.ZS", "health expenditure")
        edu = await self._fetch_mean(db, "SE.XPD.TOTL.GD.ZS", "education expenditure")
        transfers = await self._fetch_mean(db, "GC.XPN.TRFT.ZS", "social transfers")

        if all(v is None for v in [health, edu, transfers]):
            return {"score": None, "signal": "UNAVAILABLE", "error": "no coverage data available"}

        # Normalize each pillar: ratio vs benchmark, capped at 1.0
        scores = []
        if health is not None:
            scores.append(min(1.0, health / HEALTH_BENCH))
        if edu is not None:
            scores.append(min(1.0, edu / EDU_BENCH))
        if transfers is not None:
            scores.append(min(1.0, transfers / TRANSFER_BENCH))

        coverage_ratio = float(np.mean(scores))
        # Score: 0 = fully universal, 100 = no coverage
        score = float(np.clip((1.0 - coverage_ratio) * 100, 0, 100))

        return {
            "score": round(score, 1),
            "health_pct_gdp": round(health, 2) if health is not None else None,
            "education_pct_gdp": round(edu, 2) if edu is not None else None,
            "social_transfers_pct_gdp": round(transfers, 2) if transfers is not None else None,
            "pillars_available": len(scores),
            "coverage_ratio": round(coverage_ratio, 3),
            "interpretation": (
                "very narrow coverage" if score > 75
                else "limited coverage" if score > 50
                else "moderate universalism" if score > 25
                else "broad universalism"
            ),
            "sources": ["WDI SH.XPD.CHEX.GD.ZS", "WDI SE.XPD.TOTL.GD.ZS", "WDI GC.XPN.TRFT.ZS"],
        }
