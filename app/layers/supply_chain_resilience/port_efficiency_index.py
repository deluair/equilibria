"""Port efficiency index: tracking and tracing quality from LPI.

Uses LP.LPI.TRAC.XQ (ability to track and trace consignments, 1-5 scale). This
sub-component of the LPI captures port and customs efficiency relevant to supply
chain throughput.

Methodology:
    Fetch up to 15 observations of LP.LPI.TRAC.XQ. Use the most recent value.
    score = clip((5 - trac_value) / 4 * 100, 0, 100).

    TRAC = 5.0: score = 0 (world-class tracking, no gap).
    TRAC = 1.0: score = 100 (no tracking capability).
    TRAC = 2.5 (mid-range): score = 62.5.

Score (0-100): Higher score indicates lower port efficiency and tracking quality.

References:
    World Bank Logistics Performance Index sub-component LP.LPI.TRAC.XQ.
    Arvis et al. (2018). "Connecting to Compete 2018." World Bank.
    Martinkenaite & Breunig (2010). "Port efficiency and supply chain performance."
"""

from __future__ import annotations

from app.layers.base import LayerBase

_CODE = "LP.LPI.TRAC.XQ"
_NAME = "tracking and tracing"


class PortEfficiencyIndex(LayerBase):
    layer_id = "lSR"
    name = "Port Efficiency Index"

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
                "error": f"no data for {_CODE} (tracking and tracing LPI sub-index)",
            }

        trac = values[0]
        trac_clamped = max(1.0, min(5.0, trac))
        score = float((5.0 - trac_clamped) / 4.0 * 100.0)

        tier = (
            "excellent" if trac >= 4.0
            else "good" if trac >= 3.0
            else "fair" if trac >= 2.0
            else "poor"
        )

        return {
            "score": round(score, 2),
            "tracking_tracing_score": round(trac, 3),
            "efficiency_tier": tier,
            "n_obs": len(values),
            "indicator": _CODE,
        }
