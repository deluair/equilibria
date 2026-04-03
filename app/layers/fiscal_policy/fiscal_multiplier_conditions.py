"""Fiscal Multiplier Conditions module.

Assesses whether macroeconomic conditions are conducive to an effective fiscal
multiplier. High public debt and elevated inflation both compress the multiplier:
debt limits deficit financing while inflation reduces the real value of stimulus.

Methodology:
- Query GC.DOD.TOTL.GD.ZS (government debt, % GDP).
- Query FP.CPI.TOTL.ZG (CPI inflation, annual %).
- Debt penalty:  debt_component = debt / 100 * 50
- Inflation penalty: inflation_component = max(0, inflation - 3) * 5
- Score = clip(debt_component + inflation_component, 0, 100).

Sources: World Bank WDI (GC.DOD.TOTL.GD.ZS, FP.CPI.TOTL.ZG)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FiscalMultiplierConditions(LayerBase):
    layer_id = "lFP"
    name = "Fiscal Multiplier Conditions"

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
        inflation = await _latest("FP.CPI.TOTL.ZG")

        if debt is None and inflation is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        d = debt or 0.0
        inf = inflation or 0.0

        debt_component = float(d / 100 * 50)
        inflation_component = float(max(0.0, inf - 3.0) * 5)

        score = float(np.clip(debt_component + inflation_component, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "debt_pct_gdp": round(d, 3),
            "inflation_pct": round(inf, 3),
            "debt_component": round(debt_component, 3),
            "inflation_component": round(inflation_component, 3),
            "low_multiplier_conditions": score > 50,
            "indicators": ["GC.DOD.TOTL.GD.ZS", "FP.CPI.TOTL.ZG"],
        }
