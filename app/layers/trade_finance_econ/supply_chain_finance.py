"""Supply Chain Finance module.

Composite of private credit depth and logistics performance as a proxy
for supply chain finance capacity. Effective SCF (payables/receivables
financing, dynamic discounting, factoring) requires both adequate credit
markets and efficient logistics infrastructure to be viable.

Sources: WDI FS.AST.PRVT.GD.ZS (domestic credit to private sector % GDP),
         WDI LP.LPI.OVRL.XQ (Logistics Performance Index overall score, 1-5)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SupplyChainFinance(LayerBase):
    layer_id = "lTF"
    name = "Supply Chain Finance"

    async def compute(self, db, **kwargs) -> dict:
        credit_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("FS.AST.PRVT.GD.ZS", "%private%credit%GDP%"),
        )
        lpi_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("LP.LPI.OVRL.XQ", "%logistics%performance%overall%"),
        )

        if not credit_rows and not lpi_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no supply chain finance proxy data"}

        credit_pct = float(credit_rows[0]["value"]) if credit_rows else 50.0
        lpi_score = float(lpi_rows[0]["value"]) if lpi_rows else 2.5

        # Credit: 0-100% GDP normalized (>100% = good)
        credit_norm = float(np.clip(credit_pct / 100.0, 0, 1))
        # LPI: 1-5 scale, 5 = best
        lpi_norm = float(np.clip((lpi_score - 1.0) / 4.0, 0, 1))

        scf_capacity = (0.5 * credit_norm + 0.5 * lpi_norm) * 100  # 0-100, higher = better

        # Stress score: invert
        score = float(np.clip(100 - scf_capacity, 0, 100))

        return {
            "score": round(score, 2),
            "private_credit_pct_gdp": round(credit_pct, 2) if credit_rows else None,
            "lpi_overall_score": round(lpi_score, 3) if lpi_rows else None,
            "scf_capacity_index": round(scf_capacity, 2),
            "interpretation": "Low credit depth and logistics quality constrain supply chain finance viability",
        }
