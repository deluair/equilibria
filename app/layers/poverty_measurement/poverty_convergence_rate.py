"""Poverty Convergence Rate module.

Estimates the speed at which extreme poverty is being reduced using a simple
log-linear trend on the headcount poverty rate (SI.POV.DDAY). A fast
convergence rate (large negative slope in log space) implies rapid progress
toward zero poverty; stagnation or reversal signals crisis.

Score = clip(50 - annual_pct_change * 5, 0, 100).

Sources: WDI (SI.POV.DDAY)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PovertyConvergenceRate(LayerBase):
    layer_id = "lPM"
    name = "Poverty Convergence Rate"

    async def compute(self, db, **kwargs) -> dict:
        code = "SI.POV.DDAY"
        name = "poverty headcount"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for SI.POV.DDAY"}

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if len(values) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient observations for trend"}

        # Filter positive values for log-linear regression
        pos = [(i, v) for i, v in enumerate(values) if v > 0]
        if len(pos) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "too few positive observations for log trend"}

        t_arr = np.array([p[0] for p in pos], dtype=float)
        log_v = np.log([p[1] for p in pos])
        slope, intercept = np.polyfit(t_arr, log_v, 1)

        # Annual % change in headcount (slope in log space approximates pct change)
        annual_pct_change = float(slope * 100)

        # Negative slope = progress (reduction), positive = regression
        score = float(np.clip(50 - annual_pct_change * 5, 0, 100))

        return {
            "score": round(score, 1),
            "annual_pct_change": round(annual_pct_change, 3),
            "log_linear_slope": round(float(slope), 5),
            "latest_headcount_pct": round(values[0], 3),
            "n_obs": len(values),
            "n_obs_positive": len(pos),
            "indicator": code,
            "interpretation": (
                "negative annual_pct_change = poverty falling (good); "
                "positive = poverty rising (bad)"
            ),
        }
