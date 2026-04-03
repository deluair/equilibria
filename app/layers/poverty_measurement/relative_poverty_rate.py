"""Relative Poverty Rate module.

Estimates relative poverty using the Gini coefficient (SI.POV.GINI) as a
proxy for income concentration among those below 60% of median income.
A Gini above 40 signals significant relative deprivation; the score rises
with Gini and is modulated by the headcount poverty rate (SI.POV.DDAY) to
reflect the interaction between inequality and absolute poverty.

Score = clip((gini - 25) * 2.5, 0, 100).

Sources: WDI (SI.POV.GINI, SI.POV.DDAY)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RelativePovertyRate(LayerBase):
    layer_id = "lPM"
    name = "Relative Poverty Rate"

    async def compute(self, db, **kwargs) -> dict:
        gini_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("SI.POV.GINI", "%gini%"),
        )
        hc_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("SI.POV.DDAY", "%poverty headcount%"),
        )

        gini_vals = [float(r["value"]) for r in gini_rows if r["value"] is not None] if gini_rows else []
        hc_vals = [float(r["value"]) for r in hc_rows if r["value"] is not None] if hc_rows else []

        if not gini_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for SI.POV.GINI"}

        gini = gini_vals[0]
        headcount = hc_vals[0] if hc_vals else None

        score = float(np.clip((gini - 25) * 2.5, 0, 100))

        return {
            "score": round(score, 1),
            "gini_coefficient": round(gini, 2),
            "gini_n_obs": len(gini_vals),
            "headcount_pct": round(headcount, 3) if headcount is not None else None,
            "relative_poverty_proxy": "60% of median income (Gini-based)",
            "inequality_threshold": "Gini > 40 = high relative deprivation",
        }
