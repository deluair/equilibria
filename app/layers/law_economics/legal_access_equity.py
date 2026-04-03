"""Legal Access Equity module.

Captures inequality in access to legal systems by combining income inequality
(SI.POV.GINI) with Rule of Law quality (RL.EST). High inequality and weak rule
of law jointly signal that legal access is skewed toward elites.

Score formula:
  score_gini = clip(gini / 100 * 100, 0, 100)   [already 0-100 proxy: higher = worse]
  score_rl   = clip(50 - rl_latest * 20, 0, 100)
  composite  = 0.5 * score_gini + 0.5 * score_rl  (or single if one unavailable)

Higher score = greater legal access inequality / higher stress.

Sources: World Bank WDI (SI.POV.GINI, RL.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_GINI_CODE = "SI.POV.GINI"
_RL_CODE = "RL.EST"
_GINI_NAME = "Gini index"


class LegalAccessEquity(LayerBase):
    layer_id = "lLW"
    name = "Legal Access Equity"

    async def compute(self, db, **kwargs) -> dict:
        gini_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_GINI_CODE, f"%{_GINI_NAME}%"),
        )

        rl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_RL_CODE, f"%{_RL_CODE}%"),
        )

        gini_vals = [float(r["value"]) for r in gini_rows if r["value"] is not None]
        rl_vals = [float(r["value"]) for r in rl_rows if r["value"] is not None]

        if not gini_vals and not rl_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for SI.POV.GINI or RL.EST",
            }

        score_gini = float(np.clip(gini_vals[0], 0.0, 100.0)) if gini_vals else None
        score_rl = float(np.clip(50.0 - rl_vals[0] * 20.0, 0.0, 100.0)) if rl_vals else None

        if score_gini is not None and score_rl is not None:
            score = 0.5 * score_gini + 0.5 * score_rl
        elif score_gini is not None:
            score = score_gini
        else:
            score = score_rl

        return {
            "score": round(score, 1),
            "gini_index": round(gini_vals[0], 2) if gini_vals else None,
            "rl_latest": round(rl_vals[0], 4) if rl_vals else None,
            "score_gini_component": round(score_gini, 2) if score_gini is not None else None,
            "score_rl_component": round(score_rl, 2) if score_rl is not None else None,
            "indicators_used": (
                ([_GINI_CODE] if gini_vals else []) + ([_RL_CODE] if rl_vals else [])
            ),
            "note": "Higher score = more unequal legal access. GINI (0-100) + RL.EST inverted.",
        }
