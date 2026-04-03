"""Logistics performance gap: inverted logistics performance index.

Uses LP.LPI.OVRL.XQ (overall logistics performance index, 1-5 scale). A lower
LPI score signals a larger logistics gap that undermines supply chain resilience.
The score is inverted so that higher output means greater stress.

Methodology:
    Fetch up to 15 observations of LP.LPI.OVRL.XQ. Use the most recent value.
    score = clip((5 - lpi_value) / 4 * 100, 0, 100).

    LPI = 5.0: score = 0 (world-class logistics, no gap).
    LPI = 1.0: score = 100 (worst logistics, maximum gap).
    LPI = 2.6 (world avg ~2019): score = 60.

Score (0-100): Higher score indicates larger logistics performance gap.

References:
    World Bank Logistics Performance Index (LP.LPI.OVRL.XQ).
    Arvis et al. (2018). "Connecting to Compete." World Bank.
"""

from __future__ import annotations

from app.layers.base import LayerBase

_CODE = "LP.LPI.OVRL.XQ"
_NAME = "logistics performance index"


class LogisticsPerformanceGap(LayerBase):
    layer_id = "lSR"
    name = "Logistics Performance Gap"

    async def compute(self, db, **kwargs) -> dict:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_CODE, f"%{_NAME}%"),
        )

        values = [float(r["value"]) for r in rows if r["value"] is not None]

        if not values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"no data for {_CODE} (logistics performance index)",
            }

        lpi = values[0]
        lpi_clamped = max(1.0, min(5.0, lpi))
        score = float((5.0 - lpi_clamped) / 4.0 * 100.0)

        tier = (
            "world_class" if lpi >= 4.0
            else "above_average" if lpi >= 3.0
            else "below_average" if lpi >= 2.0
            else "poor"
        )

        return {
            "score": round(score, 2),
            "lpi_score": round(lpi, 3),
            "lpi_tier": tier,
            "n_obs": len(values),
            "indicator": _CODE,
        }
