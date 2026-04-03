"""Banking crisis probability module.

NPL ratio and private credit boom as leading indicators of banking distress.
Uses FB.AST.NPER.ZS (NPL ratio) and FS.AST.PRVT.GD.ZS (private credit to GDP).

Score (0-100): rising NPLs and credit boom push toward CRISIS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

NPL_CODE = "FB.AST.NPER.ZS"
NPL_NAME = "bank nonperforming loans"
CREDIT_CODE = "FS.AST.PRVT.GD.ZS"
CREDIT_NAME = "domestic credit private sector"


class BankingCrisisProbability(LayerBase):
    layer_id = "lFC"
    name = "Banking Crisis Probability"

    async def compute(self, db, **kwargs) -> dict:
        npl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (NPL_CODE, f"%{NPL_NAME}%"),
        )
        credit_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (CREDIT_CODE, f"%{CREDIT_NAME}%"),
        )

        if not npl_rows and not credit_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no banking crisis data"}

        npl_vals = [float(r["value"]) for r in npl_rows if r["value"] is not None]
        credit_vals = [float(r["value"]) for r in credit_rows if r["value"] is not None]

        npl_latest = npl_vals[0] if npl_vals else None
        credit_latest = credit_vals[0] if credit_vals else None

        # NPL component: <2% healthy, >10% crisis
        npl_score = 30.0
        if npl_latest is not None:
            npl_score = float(np.clip(npl_latest * 8.0, 0, 100))

        # Credit boom: >80% GDP is elevated, >120% is crisis-zone
        credit_score = 30.0
        if credit_latest is not None:
            credit_score = float(np.clip((credit_latest - 40.0) * 1.5, 0, 100))

        # Credit acceleration: rapid growth in recent years is a warning signal
        accel_score = 0.0
        if len(credit_vals) >= 4:
            recent = np.mean(credit_vals[:2])
            earlier = np.mean(credit_vals[2:4])
            if earlier > 0:
                growth_rate = (recent - earlier) / earlier * 100
                accel_score = float(np.clip(growth_rate * 2.0, 0, 40))

        score = float(np.clip(0.50 * npl_score + 0.35 * credit_score + 0.15 * accel_score, 0, 100))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "npl_ratio_pct": round(npl_latest, 2) if npl_latest is not None else None,
                "private_credit_gdp_pct": round(credit_latest, 2) if credit_latest is not None else None,
                "npl_score": round(npl_score, 2),
                "credit_score": round(credit_score, 2),
                "acceleration_score": round(accel_score, 2),
            },
        }
