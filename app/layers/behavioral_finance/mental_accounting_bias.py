"""Mental Accounting Bias module.

Remittance use efficiency as a proxy for mental accounting. When remittance
inflows are high but investment rates remain low, households mentally
earmark remittances for consumption rather than investment, reflecting
compartmentalized mental accounting.

Sources: WDI BX.TRF.PWKR.DT.GD.ZS (personal remittances received % of GDP),
         WDI NE.GDI.TOTL.ZS (gross capital formation % of GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MentalAccountingBias(LayerBase):
    layer_id = "lBF"
    name = "Mental Accounting Bias"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        remit_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("BX.TRF.PWKR.DT.GD.ZS", "%remittance%received%"),
        )
        invest_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("NE.GDI.TOTL.ZS", "%gross capital formation%"),
        )

        if not remit_rows or not invest_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        remit_val = float(remit_rows[0]["value"])
        invest_val = float(invest_rows[0]["value"])

        # Efficiency gap: high remittance but low investment = mental accounting
        efficiency_gap = remit_val - (invest_val * 0.1)  # expect 10% of investment from remittances
        score = float(np.clip(max(0.0, efficiency_gap) * 8, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "remittances_pct_gdp": round(remit_val, 3),
            "gross_capital_formation_pct_gdp": round(invest_val, 2),
            "efficiency_gap": round(efficiency_gap, 3),
            "interpretation": "High remittances paired with low investment suggests mental accounting earmarking for consumption",
        }
