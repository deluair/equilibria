"""Currency Risk Hedging module.

Reserve adequacy as a proxy for the economy's capacity to hedge currency
risk in trade finance. Adequate reserves underpin exchange rate stability,
reduce hedging costs, and allow the central bank to provide FX liquidity
to the banking sector for trade finance operations.

Source: WDI FI.RES.TOTL.MO (total reserves in months of imports)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CurrencyRiskHedging(LayerBase):
    layer_id = "lTF"
    name = "Currency Risk Hedging"

    async def compute(self, db, **kwargs) -> dict:
        reserve_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("FI.RES.TOTL.MO", "%reserves%months%import%"),
        )

        if not reserve_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no reserve adequacy data"}

        reserves_months = float(reserve_rows[0]["value"])

        # IMF adequacy benchmark: 3 months = minimum, 6+ months = comfortable
        # Low reserves => high currency risk => high stress score
        if reserves_months >= 6.0:
            score = 0.0
        elif reserves_months >= 3.0:
            score = float(np.clip((6.0 - reserves_months) / 3.0 * 40, 0, 40))
        else:
            score = float(np.clip(40 + (3.0 - reserves_months) / 3.0 * 60, 0, 100))

        return {
            "score": round(score, 2),
            "reserves_months_imports": round(reserves_months, 2),
            "imf_minimum_benchmark_months": 3.0,
            "adequacy_status": (
                "comfortable" if reserves_months >= 6.0
                else "adequate" if reserves_months >= 3.0
                else "below minimum"
            ),
            "interpretation": "Low import cover reduces hedging capacity and increases FX risk for trade finance",
        }
