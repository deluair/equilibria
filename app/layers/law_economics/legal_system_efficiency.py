"""Legal System Efficiency module.

Measures how efficiently a legal system resolves commercial disputes by tracking
contract enforcement duration. Shorter enforcement time = lower stress score.

Indicator: IC.LGL.DURS (time to enforce a contract, days). Inverted so that
longer enforcement time produces a higher (worse) stress score.

Score formula:
  Benchmark: 100 days = 0 (ideal), 1500+ days = 100 (worst).
  score = clip((days - 100) / 14, 0, 100)

Sources: World Bank Doing Business / WDI (IC.LGL.DURS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_INDICATOR_CODE = "IC.LGL.DURS"
_INDICATOR_NAME = "contract enforcement days"


class LegalSystemEfficiency(LayerBase):
    layer_id = "lLW"
    name = "Legal System Efficiency"

    async def compute(self, db, **kwargs) -> dict:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_INDICATOR_CODE, f"%{_INDICATOR_NAME}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for IC.LGL.DURS"}

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid values"}

        latest = values[0]
        # Invert: longer = worse. 100 days ideal, 1500 days = worst.
        score = float(np.clip((latest - 100.0) / 14.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "enforcement_days": round(latest, 1),
            "n_obs": len(values),
            "note": "IC.LGL.DURS inverted: fewer days = lower stress score",
        }
