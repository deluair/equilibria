"""Judicial Independence Index module.

Measures structural stress on judicial independence using Rule of Law (RL.EST)
and Voice & Accountability (VA.EST). Both are inverted to represent stress:
deteriorating RL and falling VA signal weaker judicial independence.

Score formula:
  score_rl = clip(50 - rl_latest * 20, 0, 100)
  score_va = clip(50 - va_latest * 20, 0, 100)
  composite = 0.6 * score_rl + 0.4 * score_va  (or single if one unavailable)

Higher score = higher judicial independence stress.

Sources: World Bank WDI (RL.EST, VA.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_RL_CODE = "RL.EST"
_VA_CODE = "VA.EST"


class JudicalIndependenceIndex(LayerBase):
    layer_id = "lLW"
    name = "Judicial Independence Index"

    async def compute(self, db, **kwargs) -> dict:
        rl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_RL_CODE, f"%{_RL_CODE}%"),
        )

        va_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_VA_CODE, f"%{_VA_CODE}%"),
        )

        rl_vals = [float(r["value"]) for r in rl_rows if r["value"] is not None]
        va_vals = [float(r["value"]) for r in va_rows if r["value"] is not None]

        if not rl_vals and not va_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for RL.EST or VA.EST"}

        score_rl = float(np.clip(50.0 - rl_vals[0] * 20.0, 0.0, 100.0)) if rl_vals else None
        score_va = float(np.clip(50.0 - va_vals[0] * 20.0, 0.0, 100.0)) if va_vals else None

        if score_rl is not None and score_va is not None:
            score = 0.6 * score_rl + 0.4 * score_va
        elif score_rl is not None:
            score = score_rl
        else:
            score = score_va

        return {
            "score": round(score, 1),
            "rl_latest": round(rl_vals[0], 4) if rl_vals else None,
            "va_latest": round(va_vals[0], 4) if va_vals else None,
            "score_rl_component": round(score_rl, 2) if score_rl is not None else None,
            "score_va_component": round(score_va, 2) if score_va is not None else None,
            "indicators_used": (
                ([_RL_CODE] if rl_vals else []) + ([_VA_CODE] if va_vals else [])
            ),
            "note": "RL.EST + VA.EST inverted to stress. Higher = weaker judicial independence.",
        }
