"""House Price to Income Ratio module.

Proxies price-to-income stress via GDP per capita (NY.GDP.PCAP.KD) as income
proxy and CPI (FP.CPI.TOTL.ZG) + urban population (SP.URB.TOTL.IN.ZS) to
model relative house price pressure. A sustained wedge between cumulative
price inflation and income growth signals affordability deterioration.

Score = clip((cum_price_inflation - cum_income_growth + 50), 0, 100)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HousePriceIncomeRatio(LayerBase):
    layer_id = "lHO"
    name = "House Price to Income Ratio"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        income_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY dp.date
            """,
            (country,),
        )

        cpi_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FP.CPI.TOTL.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not income_rows or len(income_rows) < 5 or not cpi_rows or len(cpi_rows) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data for house price to income ratio",
            }

        income_vals = np.array([float(r["value"]) for r in income_rows])
        cpi_vals = np.array([float(r["value"]) for r in cpi_rows])

        # Annualised income growth over last 5 obs
        income_growth = np.diff(income_vals) / (np.abs(income_vals[:-1]) + 1e-10) * 100
        avg_income_growth = float(np.mean(income_growth[-5:]))

        # Cumulative CPI inflation over last 5 obs (average annual rate)
        avg_inflation = float(np.mean(cpi_vals[-5:]))

        # Positive gap: prices rising faster than incomes = stress
        gap = avg_inflation - avg_income_growth
        raw_score = gap + 50
        score = float(np.clip(raw_score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "avg_income_growth_pct": round(avg_income_growth, 2),
            "avg_cpi_inflation_pct": round(avg_inflation, 2),
            "price_income_gap": round(gap, 2),
            "n_income_obs": len(income_rows),
            "n_cpi_obs": len(cpi_rows),
            "methodology": "score = clip((avg_cpi - avg_income_growth + 50), 0, 100)",
        }
