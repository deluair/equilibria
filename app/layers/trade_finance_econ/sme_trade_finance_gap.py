"""SME Trade Finance Gap module.

Proxy for the structural trade finance gap faced by SMEs, combining
business environment quality and private credit depth relative to trade
share. Where doing business is hard and credit is shallow relative to
trade volume, SMEs face the steepest barriers to accessing trade finance
(IFC estimates 40-50% of global trade finance rejections fall on SMEs).

Sources: WDI IC.BUS.EASE.XQ (ease of doing business score, 0-100),
         WDI FS.AST.PRVT.GD.ZS (domestic credit to private sector % GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SmeTradeFinanceGap(LayerBase):
    layer_id = "lTF"
    name = "SME Trade Finance Gap"

    async def compute(self, db, **kwargs) -> dict:
        eodb_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("IC.BUS.EASE.XQ", "%ease%doing%business%"),
        )
        credit_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("FS.AST.PRVT.GD.ZS", "%private%credit%GDP%"),
        )

        if not eodb_rows and not credit_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no SME finance gap proxy data"}

        eodb_score = float(eodb_rows[0]["value"]) if eodb_rows else 50.0
        credit_pct = float(credit_rows[0]["value"]) if credit_rows else 30.0

        # EODB: 0-100, higher = better. Invert for friction.
        eodb_friction = float(np.clip(100 - eodb_score, 0, 100))
        # Credit depth: <30% GDP = shallow, >100% = deep
        credit_norm = float(np.clip(credit_pct / 100.0, 0, 1))
        credit_gap = float(np.clip((1 - credit_norm) * 100, 0, 100))

        # SME gap: weighted toward business environment (SME access driver)
        score = float(np.clip(0.55 * eodb_friction + 0.45 * credit_gap, 0, 100))

        return {
            "score": round(score, 2),
            "eodb_score": round(eodb_score, 2) if eodb_rows else None,
            "private_credit_pct_gdp": round(credit_pct, 2) if credit_rows else None,
            "business_friction_index": round(eodb_friction, 2),
            "credit_gap_index": round(credit_gap, 2),
            "interpretation": "Poor business environment and shallow credit markets amplify the SME trade finance gap",
        }
