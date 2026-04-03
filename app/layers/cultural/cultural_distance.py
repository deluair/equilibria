"""Cultural Distance module.

Export diversification as a proxy for cultural openness and international
economic integration. Countries with a narrow range of active trade
relationships exhibit cultural isolation from global markets.

Method: count the number of distinct trade-related series (TX, BX, TM)
with at least one non-zero observation. A low count signals limited
partner/product diversity.

Benchmark: >= 10 distinct active series -> fully diversified (score = 0).
score = clip((10 - n_active) / 10 * 100, 0, 100)
- n_active = 10 -> score = 0
- n_active = 5  -> score = 50
- n_active = 0  -> score = 100

Sources: WDI (TX.*, BX.*, TM.* series), Comtrade (if available)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

DIVERSITY_BENCHMARK = 10


class CulturalDistance(LayerBase):
    layer_id = "lCU"
    name = "Cultural Distance"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND (ds.series_id LIKE 'TX.%' OR ds.series_id LIKE 'BX.%'
                   OR ds.series_id LIKE 'TM.%')
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data (need >= 5 rows)",
            }

        # Count distinct series with at least one non-zero value
        series_nonzero: set[str] = set()
        series_all: set[str] = set()
        for r in rows:
            sid = r["series_id"]
            series_all.add(sid)
            if float(r["value"]) != 0.0:
                series_nonzero.add(sid)

        n_active = len(series_nonzero)
        n_total = len(series_all)

        score = float(np.clip(
            (DIVERSITY_BENCHMARK - n_active) / DIVERSITY_BENCHMARK * 100.0, 0.0, 100.0
        ))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(rows),
            "n_active_series": n_active,
            "n_total_series": n_total,
            "benchmark_series_count": DIVERSITY_BENCHMARK,
            "note": (
                "score = clip((10 - n_active)/10*100, 0, 100); "
                "low n_active = low export diversity = cultural isolation"
            ),
        }
