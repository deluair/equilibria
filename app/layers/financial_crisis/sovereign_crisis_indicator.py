"""Sovereign crisis indicator module.

External debt to GNI (DT.DOD.DECT.GD.ZS) and cash fiscal balance (GC.BAL.CASH.GD.ZS)
as leading indicators of sovereign debt distress.

Score (0-100): high external debt and fiscal deficits push toward CRISIS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

DEBT_CODE = "DT.DOD.DECT.GD.ZS"
DEBT_NAME = "external debt stocks"
FISCAL_CODE = "GC.BAL.CASH.GD.ZS"
FISCAL_NAME = "cash surplus deficit"


class SovereignCrisisIndicator(LayerBase):
    layer_id = "lFC"
    name = "Sovereign Crisis Indicator"

    async def compute(self, db, **kwargs) -> dict:
        debt_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (DEBT_CODE, f"%{DEBT_NAME}%"),
        )
        fiscal_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (FISCAL_CODE, f"%{FISCAL_NAME}%"),
        )

        if not debt_rows and not fiscal_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no sovereign crisis data"}

        debt_vals = [float(r["value"]) for r in debt_rows if r["value"] is not None]
        fiscal_vals = [float(r["value"]) for r in fiscal_rows if r["value"] is not None]

        debt_latest = debt_vals[0] if debt_vals else None
        fiscal_latest = fiscal_vals[0] if fiscal_vals else None

        # Debt/GNI: <30% low risk, 30-60% moderate, >60% elevated, >100% crisis
        debt_score = 30.0
        if debt_latest is not None:
            debt_score = float(np.clip((debt_latest - 30.0) * 1.2, 0, 100))

        # Fiscal balance: surplus = 0, deficit > -5% = watch, > -10% = crisis
        fiscal_score = 20.0
        if fiscal_latest is not None:
            fiscal_score = float(np.clip(-fiscal_latest * 8.0, 0, 100))

        # Debt trajectory: rising external debt compounds risk
        traj_score = 0.0
        if len(debt_vals) >= 3:
            slope = (debt_vals[0] - debt_vals[min(4, len(debt_vals) - 1)])
            traj_score = float(np.clip(slope * 1.5, 0, 30))

        score = float(np.clip(0.55 * debt_score + 0.35 * fiscal_score + 0.10 * traj_score, 0, 100))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "external_debt_gni_pct": round(debt_latest, 2) if debt_latest is not None else None,
                "fiscal_balance_gdp_pct": round(fiscal_latest, 2) if fiscal_latest is not None else None,
                "debt_score": round(debt_score, 2),
                "fiscal_score": round(fiscal_score, 2),
                "trajectory_score": round(traj_score, 2),
            },
        }
