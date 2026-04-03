"""Tax Progressivity module.

Measures the effectiveness of progressive taxation by examining income tax's
share of total tax revenue, compared to total tax burden as % of GDP.

A progressive tax system relies heavily on income taxes (which rise with
income) rather than regressive consumption taxes (VAT, excise). Low income
tax share signals structural regressivity that perpetuates post-tax inequality.

Indicators:
- GC.TAX.YPKG.ZS: Taxes on income, profits and capital gains (% of revenue)
- GC.TAX.TOTL.GD.ZS: Tax revenue (% of GDP)

Score = max(0, 30 - income_tax_share) * 2
  - income_tax_share = 30%: score = 0 (neutral / moderately progressive)
  - income_tax_share = 15%: score = 30 (moderate stress)
  - income_tax_share = 0%: score = 60 (highly regressive)
  Cap at 100. Tax revenue level adds a secondary penalty when low (< 10% GDP
  = weak state capacity to redistribute).

Sources: World Bank WDI (GC.TAX.YPKG.ZS, GC.TAX.TOTL.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TaxProgressivity(LayerBase):
    layer_id = "lIQ"
    name = "Tax Progressivity"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        income_tax_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.TAX.YPKG.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        total_tax_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.TAX.TOTL.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not income_tax_rows and not total_tax_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        income_tax_share = float(income_tax_rows[0]["value"]) if income_tax_rows else 20.0
        total_tax_gdp = float(total_tax_rows[0]["value"]) if total_tax_rows else 15.0
        has_income_tax = bool(income_tax_rows)
        has_total_tax = bool(total_tax_rows)

        # Primary score: low income tax share = regressive
        progressivity_score = float(max(0.0, 30.0 - income_tax_share) * 2.0)

        # Secondary: low overall tax revenue = weak redistribution capacity
        capacity_penalty = float(np.clip((10.0 - total_tax_gdp) * 2.0, 0, 20))

        score = float(np.clip(progressivity_score + capacity_penalty, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "income_tax_pct_revenue": round(income_tax_share, 2),
            "total_tax_pct_gdp": round(total_tax_gdp, 2),
            "income_tax_source": "observed" if has_income_tax else "imputed_default",
            "total_tax_source": "observed" if has_total_tax else "imputed_default",
            "progressivity_score": round(progressivity_score, 2),
            "capacity_penalty": round(capacity_penalty, 2),
            "interpretation": {
                "regressive_structure": income_tax_share < 20,
                "low_tax_capacity": total_tax_gdp < 10,
                "benchmark_income_tax_share_pct": 30.0,
            },
        }
