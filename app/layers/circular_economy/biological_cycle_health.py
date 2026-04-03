"""Biological cycle health: agricultural land health and arable land trend.

Monitors arable land per capita (AG.LND.ARBL.HA.PC) over time as an indicator
of biological cycle health. Declining arable land per capita signals soil
degradation, urban encroachment, and weakening of biological nutrient cycles
that underpin a circular bioeconomy.

References:
    Rockstrom, J. et al. (2009). A safe operating space for humanity.
        Nature, 461, 472-475.
    FAO (2022). The State of the World's Land and Water Resources.
    World Bank WDI: AG.LND.ARBL.HA.PC
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class BiologicalCycleHealth(LayerBase):
    layer_id = "lCE"
    name = "Biological Cycle Health"

    ARABLE_PC_CODE = "AG.LND.ARBL.HA.PC"

    async def compute(self, db, **kwargs) -> dict:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (self.ARABLE_PC_CODE, f"%{self.ARABLE_PC_CODE}%"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no arable land per capita data for biological cycle health",
            }

        vals = [r["value"] for r in rows if r["value"] is not None]
        if not vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "null arable land per capita values",
            }

        latest = float(vals[0])

        # Trend: declining per capita arable = degrading biological cycles
        trend_slope = None
        pct_change = None
        if len(vals) >= 3:
            arr = np.array(vals[:10], dtype=float)
            trend_slope = float(np.polyfit(np.arange(len(arr)), arr, 1)[0])
        if len(vals) >= 2:
            oldest = float(vals[-1])
            pct_change = (latest - oldest) / oldest * 100.0 if oldest != 0 else None

        # Score: declining arable land per capita = poor biological cycle health = higher stress
        # Benchmark: global average ~0.2 ha/capita; <0.1 = severe constraint
        if latest >= 0.3:
            level_score = 10.0
        elif latest >= 0.2:
            level_score = 25.0
        elif latest >= 0.1:
            level_score = 50.0
        else:
            level_score = 75.0

        # Penalize declining trend
        trend_penalty = 0.0
        if trend_slope is not None and trend_slope < 0:
            trend_penalty = min(abs(trend_slope) * 1000.0, 25.0)

        score = float(np.clip(level_score + trend_penalty, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "arable_land_per_capita_ha": round(latest, 4),
            "arable_pc_trend_slope_ha_yr": round(trend_slope, 6) if trend_slope is not None else None,
            "arable_pc_change_pct": round(pct_change, 2) if pct_change is not None else None,
            "trend_direction": (
                "declining" if (trend_slope is not None and trend_slope < 0) else "stable_or_rising"
            ),
        }
