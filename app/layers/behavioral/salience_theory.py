"""Salience Theory module.

Salience in taxation: indirect vs direct tax ratio as attention proxy.
A high share of indirect taxes (VAT, excise) in total tax revenue signals
that the true tax burden is less visible to citizens -- consistent with
salience theory (Chetty, Looney & Kroft 2009).

Score = clip((indirect_share * 100), 0, 100) where indirect_share = 1 - (income_tax / total_tax)

Sources: WDI GC.TAX.TOTL.GD.ZS (Tax revenue, % GDP),
         WDI GC.TAX.YPKG.ZS (Taxes on income, profits and capital gains, % of revenue)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SalienceTheory(LayerBase):
    layer_id = "l13"
    name = "Salience Theory"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        total_tax_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.TAX.TOTL.GD.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        income_tax_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.TAX.YPKG.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not total_tax_rows or len(total_tax_rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient total tax data"}
        if not income_tax_rows or len(income_tax_rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient income tax data"}

        total_map = {r["date"]: float(r["value"]) for r in total_tax_rows}
        income_map = {r["date"]: float(r["value"]) for r in income_tax_rows}
        common_dates = sorted(set(total_map) & set(income_map))

        if len(common_dates) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient overlapping dates"}

        total_vals = np.array([total_map[d] for d in common_dates])
        income_vals = np.array([income_map[d] for d in common_dates])

        # income_tax is % of revenue; scale to GDP for comparison
        # indirect share = 1 - (income_tax_share_of_revenue / 100)
        income_share = float(np.mean(income_vals)) / 100.0
        indirect_share = 1.0 - income_share

        score = float(np.clip(indirect_share * 100, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(common_dates),
            "period": f"{common_dates[0]} to {common_dates[-1]}",
            "mean_total_tax_pct_gdp": round(float(np.mean(total_vals)), 2),
            "mean_income_tax_pct_revenue": round(float(np.mean(income_vals)), 2),
            "direct_tax_share": round(income_share, 4),
            "indirect_tax_share": round(indirect_share, 4),
            "interpretation": "High indirect tax share = less visible burden = salience effect obscuring true taxation",
        }
