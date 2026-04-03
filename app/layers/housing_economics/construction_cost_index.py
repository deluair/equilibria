"""Construction Cost Index module.

Building cost inflation vs income growth proxy. Uses CPI inflation
(FP.CPI.TOTL.ZG) as a general cost-of-construction proxy and GDP per
capita growth (NY.GDP.PCAP.KD) as income benchmark. When construction
costs consistently outpace income growth, housing affordability erodes
and supply expansion becomes financially unviable for developers.

Score = clip((avg_inflation - avg_income_growth) * 3 + 40, 0, 100)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ConstructionCostIndex(LayerBase):
    layer_id = "lHO"
    name = "Construction Cost Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

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

        if not cpi_rows or len(cpi_rows) < 3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient CPI data for construction cost index",
            }

        if not income_rows or len(income_rows) < 3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient income data for construction cost index",
            }

        cpi_vals = np.array([float(r["value"]) for r in cpi_rows])
        income_vals = np.array([float(r["value"]) for r in income_rows])

        avg_inflation = float(np.mean(cpi_vals[-5:])) if len(cpi_vals) >= 5 else float(np.mean(cpi_vals))

        income_growth_arr = np.diff(income_vals) / (np.abs(income_vals[:-1]) + 1e-10) * 100
        avg_income_growth = float(np.mean(income_growth_arr[-5:])) if len(income_growth_arr) >= 5 else float(np.mean(income_growth_arr))

        cost_income_gap = avg_inflation - avg_income_growth
        raw_score = cost_income_gap * 3 + 40
        score = float(np.clip(raw_score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "avg_cpi_inflation_pct": round(avg_inflation, 2),
            "avg_income_growth_pct": round(avg_income_growth, 2),
            "cost_income_gap": round(cost_income_gap, 2),
            "n_cpi_obs": len(cpi_rows),
            "n_income_obs": len(income_rows),
            "methodology": "score = clip((avg_inflation - avg_income_growth) * 3 + 40, 0, 100)",
        }
