"""Water scarcity index: freshwater withdrawal as % of available resources.

Uses ER.H2O.FWTL.ZS (annual freshwater withdrawals as % of internal resources).
High withdrawal rates signal scarcity; above 25% is water stress, above 75% is
severe scarcity (UN FAO / Falkenmark indicator thresholds).

Sources: World Bank WDI (ER.H2O.FWTL.ZS)
"""

from __future__ import annotations

from app.layers.base import LayerBase


class WaterScarcityIndex(LayerBase):
    layer_id = "lWA"
    name = "Water Scarcity Index"

    async def compute(self, db, **kwargs) -> dict:
        code = "ER.H2O.FWTL.ZS"
        name = "freshwater withdrawals"
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
                "error": "No freshwater withdrawal data found",
            }

        latest = float(vals[0])
        avg = sum(float(v) for v in vals) / len(vals)

        # Falkenmark thresholds: <25% = no stress, 25-75% = stress, >75% = severe
        if latest >= 75.0:
            score = 90.0
        elif latest >= 25.0:
            score = 40.0 + (latest - 25.0) / 50.0 * 45.0
        else:
            score = latest / 25.0 * 40.0

        score = round(min(100.0, max(0.0, score)), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "withdrawal_pct_latest": round(latest, 2),
                "withdrawal_pct_avg": round(avg, 2),
                "n_obs": len(vals),
                "stress_level": (
                    "severe" if latest >= 75 else "stressed" if latest >= 25 else "none"
                ),
            },
        }
