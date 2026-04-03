"""Essential medicines access: out-of-pocket health spending as proxy.

High OOP health spending signals limited public/insurance coverage of essential
medicines, forcing households to pay directly. Lower OOP share correlates with
better access to essential medicines through public programs.

Key references:
    WHO (2019). World Health Statistics. Essential medicines access indicators.
    Lagomarsino, G. et al. (2012). Moving towards universal health coverage.
        The Lancet, 380(9845), 861-870.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class EssentialMedicinesAccess(LayerBase):
    layer_id = "lPH"
    name = "Essential Medicines Access"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Estimate essential medicines access from OOP health spending.

        Uses SH.XPD.OOPC.CH.ZS (out-of-pocket health spending as % of CHE).
        High OOP share indicates poor financial protection and limited access
        to essential medicines via public programs.

        Returns dict with score, signal, and relevant metrics.
        """
        code = "SH.XPD.OOPC.CH.ZS"
        name = "out-of-pocket health"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"No data for {code} in DB",
            }

        values = [float(row["value"]) for row in rows if row["value"] is not None]
        if not values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "All fetched rows have NULL value",
            }

        latest = values[0]
        mean_val = float(np.mean(values))

        # High OOP = poor access. Score rises with OOP share.
        # OOP > 50%: near-crisis access (score ~80+). OOP < 15%: good access (score ~15).
        score = float(np.clip((latest / 60.0) * 100, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "oop_health_spend_pct_che_latest": round(latest, 2),
                "oop_health_spend_pct_che_mean_15obs": round(mean_val, 2),
                "n_observations": len(values),
                "indicator": code,
            },
        }
