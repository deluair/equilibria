"""Crisis recovery capacity module.

Fiscal space (GC.BAL.CASH.GD.ZS) and rule of law (RL.EST) determine how quickly
an economy can recover from a financial crisis. Fiscal surpluses and strong
institutions enable faster recovery; deficits and weak rule of law delay it.

Score (0-100): lower recovery capacity = higher vulnerability to prolonged crisis.
Note: score is inverted -- HIGH score = LOW capacity = HIGH risk.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

FISCAL_CODE = "GC.BAL.CASH.GD.ZS"
FISCAL_NAME = "cash surplus deficit"
ROL_CODE = "RL.EST"
ROL_NAME = "rule of law"


class CrisisRecoveryCapacity(LayerBase):
    layer_id = "lFC"
    name = "Crisis Recovery Capacity"

    async def compute(self, db, **kwargs) -> dict:
        fiscal_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (FISCAL_CODE, f"%{FISCAL_NAME}%"),
        )
        rol_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (ROL_CODE, f"%{ROL_NAME}%"),
        )

        if not fiscal_rows and not rol_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no recovery capacity data"}

        fiscal_vals = [float(r["value"]) for r in fiscal_rows if r["value"] is not None]
        rol_vals = [float(r["value"]) for r in rol_rows if r["value"] is not None]

        fiscal_latest = fiscal_vals[0] if fiscal_vals else None
        rol_latest = rol_vals[0] if rol_vals else None

        # Fiscal space: surplus > 0 = good capacity; deficit < -5% = limited capacity
        # Inverted: deficit = high score (low capacity = high crisis risk)
        fiscal_risk = 50.0
        if fiscal_latest is not None:
            fiscal_risk = float(np.clip(-fiscal_latest * 8.0 + 40.0, 0, 100))

        # Rule of law: WB estimate typically -2.5 to +2.5; higher = better institutions
        # Inverted: weak institutions = high risk score
        rol_risk = 50.0
        if rol_latest is not None:
            rol_risk = float(np.clip((0.5 - rol_latest) * 25.0, 0, 100))

        score = float(np.clip(0.50 * fiscal_risk + 0.50 * rol_risk, 0, 100))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "fiscal_balance_gdp_pct": round(fiscal_latest, 2) if fiscal_latest is not None else None,
                "rule_of_law_estimate": round(rol_latest, 3) if rol_latest is not None else None,
                "fiscal_capacity_risk": round(fiscal_risk, 2),
                "institutional_capacity_risk": round(rol_risk, 2),
                "interpretation": "high score = low recovery capacity = prolonged crisis risk",
            },
        }
