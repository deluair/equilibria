"""Legal Formalism Index module.

Measures procedural burden in contract enforcement through the number of
required steps. More procedures = greater formalism = higher friction = more
stress on the legal-economic nexus.

Indicator: IC.LGL.PROC.NO (number of procedures to enforce a contract).

Score formula:
  Benchmark: 20 procedures = moderate (score ~0-20), 50+ = severe (score ~100).
  score = clip((procedures - 20) / 0.3, 0, 100)

Sources: World Bank Doing Business / WDI (IC.LGL.PROC.NO)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_INDICATOR_CODE = "IC.LGL.PROC.NO"
_INDICATOR_NAME = "procedures to enforce contract"


class LegalFormalismIndex(LayerBase):
    layer_id = "lLW"
    name = "Legal Formalism Index"

    async def compute(self, db, **kwargs) -> dict:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_INDICATOR_CODE, f"%{_INDICATOR_NAME}%"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for IC.LGL.PROC.NO",
            }

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid values"}

        latest = values[0]
        # More procedures = worse. Benchmark: ~20 procedures = minimal friction.
        score = float(np.clip((latest - 20.0) / 0.3, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "procedures": round(latest, 0),
            "n_obs": len(values),
            "note": "IC.LGL.PROC.NO: more procedures = higher formalism stress.",
        }
