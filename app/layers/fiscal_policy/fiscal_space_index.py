"""Fiscal Space Index module.

Constructs a composite index of fiscal space: the ability of government to
expand spending without jeopardising debt sustainability. Low fiscal space
(high debt, structural deficit, weak tax base) translates to a high stress score.

Methodology:
- Query GC.DOD.TOTL.GD.ZS (debt, % GDP): normalised to [0,1] where 100% = max stress.
- Query GC.BAL.CASH.GD.ZS (fiscal balance, % GDP): deficit raises stress.
- Query GC.TAX.TOTL.GD.ZS (tax revenue, % GDP): low revenue raises stress.
- Composite:
    debt_component    = clip(debt / 100, 0, 1)
    balance_component = clip(-balance / 10, 0, 1)   (deficit)
    tax_component     = clip((20 - tax_pct) / 20, 0, 1)  (low revenue)
    score = (debt_component * 40 + balance_component * 35 + tax_component * 25)

Sources: World Bank WDI (GC.DOD.TOTL.GD.ZS, GC.BAL.CASH.GD.ZS, GC.TAX.TOTL.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FiscalSpaceIndex(LayerBase):
    layer_id = "lFP"
    name = "Fiscal Space Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        async def _latest(series_id: str) -> float | None:
            rows = await db.fetch_all(
                """
                SELECT dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country, series_id),
            )
            if rows:
                return float(rows[0]["value"])
            return None

        debt = await _latest("GC.DOD.TOTL.GD.ZS")
        balance = await _latest("GC.BAL.CASH.GD.ZS")
        tax = await _latest("GC.TAX.TOTL.GD.ZS")

        available = sum(x is not None for x in [debt, balance, tax])
        if available == 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        # Components (each 0-1, higher = more stressed)
        debt_c = float(np.clip((debt or 0.0) / 100, 0, 1))
        balance_c = float(np.clip(-(balance or 0.0) / 10, 0, 1))
        tax_c = float(np.clip((20 - (tax or 20.0)) / 20, 0, 1))

        # Weighted composite (weights sum to 100)
        score = debt_c * 40 + balance_c * 35 + tax_c * 25
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "debt_pct_gdp": round(debt, 3) if debt is not None else None,
            "fiscal_balance_pct_gdp": round(balance, 3) if balance is not None else None,
            "tax_revenue_pct_gdp": round(tax, 3) if tax is not None else None,
            "components": {
                "debt_stress": round(debt_c, 4),
                "deficit_stress": round(balance_c, 4),
                "revenue_stress": round(tax_c, 4),
            },
            "indicators": ["GC.DOD.TOTL.GD.ZS", "GC.BAL.CASH.GD.ZS", "GC.TAX.TOTL.GD.ZS"],
        }
