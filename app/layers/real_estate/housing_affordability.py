"""Housing Affordability module.

Proxy for house price to income ratio stress. Uses GDP per capita growth
(NY.GDP.PCAP.KD) as income proxy and CPI inflation (FP.CPI.TOTL.ZG) as
price proxy. High inflation combined with low income growth signals
affordability stress.

Score = clip(inflation - income_growth_avg + 50, 0, 100)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HousingAffordability(LayerBase):
    layer_id = "lRE"
    name = "Housing Affordability"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        gdp_rows = await db.fetch_all(
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

        if not gdp_rows or len(gdp_rows) < 3 or not cpi_rows or len(cpi_rows) < 3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data for housing affordability",
            }

        gdp_vals = np.array([float(r["value"]) for r in gdp_rows])
        cpi_vals = np.array([float(r["value"]) for r in cpi_rows])

        # Compute growth rates for GDP per capita (period-over-period % change)
        gdp_growth = np.diff(gdp_vals) / (np.abs(gdp_vals[:-1]) + 1e-10) * 100
        income_growth_avg = float(np.mean(gdp_growth[-5:])) if len(gdp_growth) >= 5 else float(np.mean(gdp_growth))

        # Latest inflation as price proxy
        inflation = float(np.mean(cpi_vals[-3:]))

        raw_score = inflation - income_growth_avg + 50
        score = float(np.clip(raw_score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "inflation_pct": round(inflation, 2),
            "income_growth_avg_pct": round(income_growth_avg, 2),
            "affordability_gap": round(inflation - income_growth_avg, 2),
            "n_gdp_obs": len(gdp_rows),
            "n_cpi_obs": len(cpi_rows),
            "methodology": "score = clip(inflation - income_growth_avg + 50, 0, 100)",
        }
