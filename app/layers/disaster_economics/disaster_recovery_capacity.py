"""Disaster Recovery Capacity module.

Measures recovery capacity using governance quality, fiscal balance, and
government expenditure. Low governance + fiscal deficit + low spending
signal constrained recovery capacity.

Indicators:
  GE.EST    -- Government effectiveness estimate (range approx -2.5 to 2.5)
  GC.BAL.CASH.GD.ZS -- Cash surplus/deficit (% GDP); negative = deficit
  GC.XPN.TOTL.GD.ZS -- Government expenditure (% GDP)

Score components (each 0-34, capped at 100):
  - governance_penalty: max(0, -GE.EST) * 20  (worse governance = higher)
  - fiscal_penalty: max(0, -balance) * 3       (larger deficit = higher)
  - spend_penalty: max(0, 20 - spend) * 1.5   (low spending = higher)

Sources: WDI (GE.EST, GC.BAL.CASH.GD.ZS, GC.XPN.TOTL.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DisasterRecoveryCapacity(LayerBase):
    layer_id = "lDE"
    name = "Disaster Recovery Capacity"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        async def _fetch(series_id: str) -> list[float]:
            rows = await db.fetch_all(
                """
                SELECT dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT 10
                """,
                (country, series_id),
            )
            return [float(r["value"]) for r in rows if r["value"] is not None]

        gov_vals = await _fetch("GE.EST")
        bal_vals = await _fetch("GC.BAL.CASH.GD.ZS")
        spend_vals = await _fetch("GC.XPN.TOTL.GD.ZS")

        if not gov_vals and not bal_vals and not spend_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        governance = float(np.mean(gov_vals)) if gov_vals else 0.0
        balance = float(np.mean(bal_vals)) if bal_vals else 0.0
        spend = float(np.mean(spend_vals)) if spend_vals else 20.0

        governance_penalty = max(0.0, -governance) * 20.0
        fiscal_penalty = max(0.0, -balance) * 3.0
        spend_penalty = max(0.0, 20.0 - spend) * 1.5
        score = float(np.clip(governance_penalty + fiscal_penalty + spend_penalty, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "governance_estimate": round(governance, 4),
            "fiscal_balance_pct_gdp": round(balance, 4),
            "govt_expenditure_pct_gdp": round(spend, 4),
            "governance_penalty": round(governance_penalty, 2),
            "fiscal_penalty": round(fiscal_penalty, 2),
            "spend_penalty": round(spend_penalty, 2),
            "indicators": {
                "governance": "GE.EST",
                "fiscal_balance": "GC.BAL.CASH.GD.ZS",
                "expenditure": "GC.XPN.TOTL.GD.ZS",
            },
        }
