"""Trade Credit Gap module.

Trade finance gap proxy combining private credit depth and trade openness.
High trade openness paired with shallow private credit implies a structural
trade finance gap -- exporters and importers cannot access adequate financing.

Sources: WDI FS.AST.PRVT.GD.ZS (domestic credit to private sector % GDP),
         WDI NE.TRD.GNFS.ZS (trade as % GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TradeCreditGap(LayerBase):
    layer_id = "lTF"
    name = "Trade Credit Gap"

    async def compute(self, db, **kwargs) -> dict:
        credit_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("FS.AST.PRVT.GD.ZS", "%private%credit%GDP%"),
        )
        trade_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("NE.TRD.GNFS.ZS", "%trade%GDP%"),
        )

        if not credit_rows or not trade_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data for trade credit gap"}

        credit_pct = float(credit_rows[0]["value"])
        trade_pct = float(trade_rows[0]["value"])

        # Gap = trade intensity relative to credit depth
        # High trade / low credit => large gap => high stress score
        ratio = trade_pct / max(credit_pct, 1.0)
        # Normalize: ratio ~0.5 is balanced, >2 indicates gap pressure
        score = float(np.clip((ratio - 0.5) / 2.0 * 100, 0, 100))

        return {
            "score": round(score, 2),
            "private_credit_pct_gdp": round(credit_pct, 2),
            "trade_pct_gdp": round(trade_pct, 2),
            "trade_to_credit_ratio": round(ratio, 3),
            "interpretation": "Higher ratio of trade intensity to credit depth implies larger trade finance gap",
        }
