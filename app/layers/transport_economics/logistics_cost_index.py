"""Logistics Cost Index module.

Inverts the World Bank Logistics Performance Index (overall) to produce a
cost/friction score. Higher LPI overall = lower logistics costs = lower score.

Indicator: LP.LPI.OVRL.XQ (LPI overall, 1-5 scale).
Score = clip((5 - lpi_overall) / 4 * 100, 0, 100).
Score=0 means best-in-class logistics; Score=100 means worst.

Sources: WDI LP.LPI.OVRL.XQ
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class LogisticsCostIndex(LayerBase):
    layer_id = "lTR"
    name = "Logistics Cost Index"

    async def compute(self, db, **kwargs) -> dict:
        code = "LP.LPI.OVRL.XQ"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{code}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for LP.LPI.OVRL.XQ"}

        lpi_overall = float(rows[0]["value"])
        score = float(np.clip((5.0 - lpi_overall) / 4.0 * 100.0, 0, 100))

        all_vals = [float(r["value"]) for r in rows]
        trend = "improving" if len(all_vals) >= 3 and all_vals[0] > all_vals[-1] else "deteriorating" if len(all_vals) >= 3 and all_vals[0] < all_vals[-1] else "stable"

        return {
            "score": round(score, 1),
            "signal": self.classify_signal(score),
            "metrics": {
                "lpi_overall": round(lpi_overall, 3),
                "lpi_scale": "1 (worst) to 5 (best)",
                "lpi_trend": trend,
                "n_obs": len(all_vals),
            },
            "_sources": ["WDI:LP.LPI.OVRL.XQ"],
        }
