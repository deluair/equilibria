"""Poverty Trap Indicator module.

Detects poverty trap risk using the trend in the $2.15/day headcount
(SI.POV.DDAY). A trap is signalled when the poverty rate is above 10% and
the 10-period OLS slope is non-negative (no meaningful decline) or when
progress has stalled. The score is elevated when both absolute poverty is
high and structural momentum is absent.

Score = base from headcount + trap premium if slope >= 0.

Sources: WDI (SI.POV.DDAY)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PovertyTrapIndicator(LayerBase):
    layer_id = "lPM"
    name = "Poverty Trap Indicator"

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
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid values"}

        latest = values[0]
        trap_detected = False
        slope = None

        if len(values) >= 3:
            t = np.arange(len(values))
            slope = float(np.polyfit(t, values, 1)[0])
            # Trap: high poverty AND no meaningful decline (slope >= -0.2 pp/period)
            trap_detected = bool(latest > 10 and slope >= -0.2)
        else:
            trap_detected = bool(latest > 10)

        base_score = float(np.clip(latest * 2, 0, 70))
        trap_premium = 25.0 if trap_detected else 0.0
        score = float(np.clip(base_score + trap_premium, 0, 100))

        return {
            "score": round(score, 1),
            "headcount_pct": round(latest, 3),
            "trend_slope_pp_per_period": round(slope, 4) if slope is not None else None,
            "trap_detected": trap_detected,
            "n_obs": len(values),
            "indicator": code,
            "trap_criteria": "headcount > 10% AND slope >= -0.2 pp/period",
        }
