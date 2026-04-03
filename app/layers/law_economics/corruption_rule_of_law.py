"""Corruption Rule of Law module.

Dual composite of Control of Corruption (CC.EST) and Rule of Law (RL.EST).
Both indicators are on the WGI -2.5 to +2.5 scale. Deterioration in either
amplifies institutional fragility.

Score formula:
  score_cc = clip(50 - cc_latest * 20, 0, 100)
  score_rl = clip(50 - rl_latest * 20, 0, 100)
  composite = 0.5 * score_cc + 0.5 * score_rl  (or single if one unavailable)

Higher score = greater corruption / weaker rule of law (higher stress).

Sources: World Bank WDI (CC.EST, RL.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_CC_CODE = "CC.EST"
_RL_CODE = "RL.EST"


class CorruptionRuleOfLaw(LayerBase):
    layer_id = "lLW"
    name = "Corruption Rule of Law"

    async def compute(self, db, **kwargs) -> dict:
        cc_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_CC_CODE, f"%{_CC_CODE}%"),
        )

        rl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_RL_CODE, f"%{_RL_CODE}%"),
        )

        cc_vals = [float(r["value"]) for r in cc_rows if r["value"] is not None]
        rl_vals = [float(r["value"]) for r in rl_rows if r["value"] is not None]

        if not cc_vals and not rl_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for CC.EST or RL.EST",
            }

        score_cc = float(np.clip(50.0 - cc_vals[0] * 20.0, 0.0, 100.0)) if cc_vals else None
        score_rl = float(np.clip(50.0 - rl_vals[0] * 20.0, 0.0, 100.0)) if rl_vals else None

        if score_cc is not None and score_rl is not None:
            score = 0.5 * score_cc + 0.5 * score_rl
        elif score_cc is not None:
            score = score_cc
        else:
            score = score_rl

        return {
            "score": round(score, 1),
            "cc_latest": round(cc_vals[0], 4) if cc_vals else None,
            "rl_latest": round(rl_vals[0], 4) if rl_vals else None,
            "score_cc_component": round(score_cc, 2) if score_cc is not None else None,
            "score_rl_component": round(score_rl, 2) if score_rl is not None else None,
            "indicators_used": (
                ([_CC_CODE] if cc_vals else []) + ([_RL_CODE] if rl_vals else [])
            ),
            "note": "CC.EST + RL.EST dual composite. Scale: -2.5 to +2.5 (inverted to stress).",
        }
