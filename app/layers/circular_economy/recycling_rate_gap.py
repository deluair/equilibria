"""Recycling rate gap: gap from 50% recycling benchmark using land/forest proxies.

Uses forest area (% of land) and arable land trends as proxies for the
health of biological material cycles. Higher forest coverage and stable
arable land signal stronger biological recycling capacity. Gap is measured
relative to a 50% circular material target (Ellen MacArthur benchmark).

References:
    Ellen MacArthur Foundation (2019). Completing the Picture: How the Circular
        Economy Tackles Climate Change.
    World Bank WDI: AG.LND.FRST.ZS, AG.LND.ARBL.ZS
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RecyclingRateGap(LayerBase):
    layer_id = "lCE"
    name = "Recycling Rate Gap"

    FOREST_CODE = "AG.LND.FRST.ZS"
    ARABLE_CODE = "AG.LND.ARBL.ZS"

    async def compute(self, db, **kwargs) -> dict:
        forest_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (self.FOREST_CODE, f"%{self.FOREST_CODE}%"),
        )
        arable_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (self.ARABLE_CODE, f"%{self.ARABLE_CODE}%"),
        )

        if not forest_rows and not arable_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no forest or arable land data for recycling rate gap",
            }

        forest_vals = [r["value"] for r in forest_rows if r["value"] is not None]
        arable_vals = [r["value"] for r in arable_rows if r["value"] is not None]

        forest_latest = float(forest_vals[0]) if forest_vals else None
        arable_latest = float(arable_vals[0]) if arable_vals else None

        # Biological recycling proxy: combined land health index (0-100)
        # Forest >30% and arable stable = good recycling capacity
        forest_score = min(forest_latest / 30.0 * 50.0, 50.0) if forest_latest is not None else 25.0
        arable_score = 0.0
        arable_trend = None
        if len(arable_vals) >= 3:
            arr = np.array(arable_vals[:10], dtype=float)
            slope = float(np.polyfit(np.arange(len(arr)), arr, 1)[0])
            arable_trend = slope
            # Stable or growing arable = better
            arable_score = 25.0 if slope >= 0 else max(0.0, 25.0 + slope * 10)
        elif arable_latest is not None:
            arable_score = 20.0

        biological_cycle_index = forest_score + arable_score  # 0-75 range

        # Recycling rate proxy: scale biological index to 0-100% recycling rate
        estimated_recycling_rate = biological_cycle_index / 75.0 * 100.0

        # Gap from 50% benchmark
        benchmark = 50.0
        recycling_gap_pp = max(0.0, benchmark - estimated_recycling_rate)

        # Score: gap from benchmark drives stress
        score = float(np.clip(recycling_gap_pp * 2.0, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "estimated_recycling_rate_pct": round(estimated_recycling_rate, 2),
            "recycling_gap_pp": round(recycling_gap_pp, 2),
            "benchmark_recycling_rate_pct": benchmark,
            "forest_area_pct": round(forest_latest, 2) if forest_latest is not None else None,
            "arable_land_pct": round(arable_latest, 2) if arable_latest is not None else None,
            "arable_trend_slope": round(arable_trend, 4) if arable_trend is not None else None,
        }
