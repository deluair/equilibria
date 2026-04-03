"""Property Rights Protection module.

Composite of Rule of Law (RL.EST) and Control of Corruption (CC.EST) to capture
the institutional backbone of property rights enforcement.

Score formula:
  score_rl  = clip(50 - rl_latest * 20, 0, 100)
  score_cc  = clip(50 - cc_latest * 20, 0, 100)
  composite = 0.55 * score_rl + 0.45 * score_cc  (or single if only one available)

Higher score = weaker property rights protection (higher stress).

Sources: World Bank WDI (RL.EST, CC.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_RL_CODE = "RL.EST"
_CC_CODE = "CC.EST"


class PropertyRightsProtection(LayerBase):
    layer_id = "lLW"
    name = "Property Rights Protection"

    async def compute(self, db, **kwargs) -> dict:
        rl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_RL_CODE, f"%{_RL_CODE}%"),
        )

        cc_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_CC_CODE, f"%{_CC_CODE}%"),
        )

        rl_vals = [float(r["value"]) for r in rl_rows if r["value"] is not None]
        cc_vals = [float(r["value"]) for r in cc_rows if r["value"] is not None]

        if not rl_vals and not cc_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for RL.EST or CC.EST"}

        score_rl = float(np.clip(50.0 - rl_vals[0] * 20.0, 0.0, 100.0)) if rl_vals else None
        score_cc = float(np.clip(50.0 - cc_vals[0] * 20.0, 0.0, 100.0)) if cc_vals else None

        if score_rl is not None and score_cc is not None:
            score = 0.55 * score_rl + 0.45 * score_cc
        elif score_rl is not None:
            score = score_rl
        else:
            score = score_cc

        return {
            "score": round(score, 1),
            "rl_latest": round(rl_vals[0], 4) if rl_vals else None,
            "cc_latest": round(cc_vals[0], 4) if cc_vals else None,
            "score_rl_component": round(score_rl, 2) if score_rl is not None else None,
            "score_cc_component": round(score_cc, 2) if score_cc is not None else None,
            "indicators_used": (
                ([_RL_CODE] if rl_vals else []) + ([_CC_CODE] if cc_vals else [])
            ),
            "note": "RL.EST + CC.EST composite. Higher = weaker property rights.",
        }
