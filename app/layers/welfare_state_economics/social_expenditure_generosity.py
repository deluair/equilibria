"""Social Expenditure Generosity module.

Measures social transfer spending as % of GDP versus OECD benchmarks.

OECD average social spending is roughly 20% of GDP. A score reflects
how far the country deviates from that benchmark, scaled 0-100 where
higher score = less generous (greater welfare deficit).

Score = clip((max(0, 20 - transfers_pct_gdp) / 20) * 100, 0, 100)

Sources: WDI GC.XPN.TRFT.ZS (social transfers as % of GDP/expense)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

OECD_BENCHMARK = 20.0  # percent of GDP


class SocialExpenditureGenerosity(LayerBase):
    layer_id = "lWS"
    name = "Social Expenditure Generosity"

    async def compute(self, db, **kwargs) -> dict:
        code = "GC.XPN.TRFT.ZS"
        name = "social transfers"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no social transfers data"}

        vals = [float(r["value"]) for r in rows if r["value"] is not None]
        if not vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid social transfers values"}

        transfers_pct = float(np.mean(vals))
        score = float(np.clip((max(0.0, OECD_BENCHMARK - transfers_pct) / OECD_BENCHMARK) * 100, 0, 100))

        return {
            "score": round(score, 1),
            "social_transfers_pct": round(transfers_pct, 2),
            "oecd_benchmark_pct": OECD_BENCHMARK,
            "generosity_gap_pct": round(max(0.0, OECD_BENCHMARK - transfers_pct), 2),
            "interpretation": (
                "very low generosity" if score > 75
                else "low generosity" if score > 50
                else "moderate generosity" if score > 25
                else "high generosity"
            ),
            "sources": ["WDI GC.XPN.TRFT.ZS"],
        }
