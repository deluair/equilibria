"""Corporate Law Quality module.

Measures the quality of the legal environment for businesses by compositing the
Ease of Doing Business score (IC.BUS.EASE.XQ, 0-100, higher = better) with Rule
of Law (RL.EST). Together they reflect both procedural quality and enforcement.

Score formula:
  score_edb = clip(100 - edb_latest, 0, 100)  [inverted to stress scale]
  score_rl  = clip(50 - rl_latest * 20, 0, 100)
  composite = 0.55 * score_edb + 0.45 * score_rl  (or single if one unavailable)

Higher score = weaker corporate law environment (higher stress).

Sources: World Bank WDI (IC.BUS.EASE.XQ, RL.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_EDB_CODE = "IC.BUS.EASE.XQ"
_RL_CODE = "RL.EST"
_EDB_NAME = "ease of doing business"


class CorporateLawQuality(LayerBase):
    layer_id = "lLW"
    name = "Corporate Law Quality"

    async def compute(self, db, **kwargs) -> dict:
        edb_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_EDB_CODE, f"%{_EDB_NAME}%"),
        )

        rl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_RL_CODE, f"%{_RL_CODE}%"),
        )

        edb_vals = [float(r["value"]) for r in edb_rows if r["value"] is not None]
        rl_vals = [float(r["value"]) for r in rl_rows if r["value"] is not None]

        if not edb_vals and not rl_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for IC.BUS.EASE.XQ or RL.EST",
            }

        score_edb = float(np.clip(100.0 - edb_vals[0], 0.0, 100.0)) if edb_vals else None
        score_rl = float(np.clip(50.0 - rl_vals[0] * 20.0, 0.0, 100.0)) if rl_vals else None

        if score_edb is not None and score_rl is not None:
            score = 0.55 * score_edb + 0.45 * score_rl
        elif score_edb is not None:
            score = score_edb
        else:
            score = score_rl

        return {
            "score": round(score, 1),
            "edb_score": round(edb_vals[0], 2) if edb_vals else None,
            "rl_latest": round(rl_vals[0], 4) if rl_vals else None,
            "score_edb_component": round(score_edb, 2) if score_edb is not None else None,
            "score_rl_component": round(score_rl, 2) if score_rl is not None else None,
            "indicators_used": (
                ([_EDB_CODE] if edb_vals else []) + ([_RL_CODE] if rl_vals else [])
            ),
            "note": "IC.BUS.EASE.XQ (0-100, inverted) + RL.EST composite.",
        }
