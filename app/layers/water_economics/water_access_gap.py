"""Water access gap: population share without safe drinking water.

Inverts SH.H2O.BASW.ZS (people using at least basic water services, % of population)
to get the access gap. Higher gap = higher risk score.

Sources: World Bank WDI (SH.H2O.BASW.ZS)
"""

from __future__ import annotations

from app.layers.base import LayerBase


class WaterAccessGap(LayerBase):
    layer_id = "lWA"
    name = "Water Access Gap"

    async def compute(self, db, **kwargs) -> dict:
        code = "SH.H2O.BASW.ZS"
        name = "basic water services"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        vals = [row["value"] for row in rows if row["value"] is not None]
        if not vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No basic water services data found",
            }

        latest_access = float(vals[0])
        gap = max(0.0, 100.0 - latest_access)
        avg_access = sum(float(v) for v in vals) / len(vals)
        avg_gap = max(0.0, 100.0 - avg_access)

        # Score = gap as a 0-100 risk index (gap of 50%+ = near max risk)
        score = round(min(100.0, gap * 2.0), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "access_pct_latest": round(latest_access, 2),
                "gap_pct_latest": round(gap, 2),
                "gap_pct_avg": round(avg_gap, 2),
                "n_obs": len(vals),
            },
        }
