"""Contract Cost Index module.

Measures the cost of enforcing a commercial contract as a share of the claim
value. High cost deters litigation and undermines legal system effectiveness.

Indicator: IC.LGL.COST.ZS (cost to enforce a contract, % of claim value).

Score formula:
  Benchmark: 5% = minimal friction (score ~0), 50%+ = severe burden (score ~100).
  score = clip(cost_pct / 0.5, 0, 100)

Sources: World Bank Doing Business / WDI (IC.LGL.COST.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_INDICATOR_CODE = "IC.LGL.COST.ZS"
_INDICATOR_NAME = "cost to enforce contract"


class ContractCostIndex(LayerBase):
    layer_id = "lLW"
    name = "Contract Cost Index"

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
                "error": "no data for IC.LGL.COST.ZS",
            }

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid values"}

        latest = values[0]
        # cost_pct is already a percentage (e.g., 30 means 30%)
        score = float(np.clip(latest / 0.5, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "cost_pct_of_claim": round(latest, 2),
            "n_obs": len(values),
            "note": "IC.LGL.COST.ZS: cost as % of claim. Higher cost = higher stress.",
        }
