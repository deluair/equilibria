"""Debt-to-Revenue Ratio module.

Measures government debt service capacity as the ratio of total public debt
to tax revenue. A ratio above 250% indicates debt overhang that constrains
fiscal flexibility and raises default risk.

Methodology:
- Query GC.DOD.TOTL.GD.ZS (government debt, % GDP).
- Query GC.TAX.TOTL.GD.ZS (tax revenue, % GDP).
- Debt/revenue ratio = debt_pct / tax_pct * 100 (both in % GDP so ratio is
  dimensionless, scaled to 100% = parity).
- Threshold: >250 = stress, >400 = crisis.
- Score = clip(ratio - 150, 0, 250) * 0.4.

Sources: World Bank WDI (GC.DOD.TOTL.GD.ZS, GC.TAX.TOTL.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DebtToRevenueRatio(LayerBase):
    layer_id = "lFP"
    name = "Debt-to-Revenue Ratio"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        async def _latest(series_id: str) -> tuple[float | None, str | None]:
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
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
                return float(rows[0]["value"]), rows[0]["date"]
            return None, None

        debt_pct, debt_date = await _latest("GC.DOD.TOTL.GD.ZS")
        tax_pct, tax_date = await _latest("GC.TAX.TOTL.GD.ZS")

        if debt_pct is None or tax_pct is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        if abs(tax_pct) < 1e-10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "zero tax revenue"}

        # Ratio: debt as multiple of annual tax revenue (%)
        ratio = (debt_pct / tax_pct) * 100

        score = float(np.clip((ratio - 150) * 0.4, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "debt_pct_gdp": round(debt_pct, 3),
            "tax_revenue_pct_gdp": round(tax_pct, 3),
            "debt_to_revenue_ratio": round(ratio, 2),
            "stress_threshold_pct": 250,
            "debt_date": debt_date,
            "tax_date": tax_date,
            "indicators": ["GC.DOD.TOTL.GD.ZS", "GC.TAX.TOTL.GD.ZS"],
        }
