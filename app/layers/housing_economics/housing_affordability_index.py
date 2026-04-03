"""Housing Affordability Index module.

Composite mortgage affordability proxy. Uses real interest rate
(FR.INR.RINR) as mortgage cost signal and GDP per capita (NY.GDP.PCAP.KD)
growth as purchasing-power proxy. A high real rate environment combined with
weak income growth yields high affordability stress.

Score = clip((real_rate * 3) - (income_growth * 2) + 50, 0, 100)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HousingAffordabilityIndex(LayerBase):
    layer_id = "lHO"
    name = "Housing Affordability Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rate_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FR.INR.RINR'
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

        if not rate_rows or len(rate_rows) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient real interest rate data for affordability index",
            }

        if not income_rows or len(income_rows) < 3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient income data for affordability index",
            }

        rate_vals = np.array([float(r["value"]) for r in rate_rows])
        income_vals = np.array([float(r["value"]) for r in income_rows])

        real_rate = float(np.mean(rate_vals[-3:]))

        income_growth_arr = np.diff(income_vals) / (np.abs(income_vals[:-1]) + 1e-10) * 100
        income_growth = float(np.mean(income_growth_arr[-3:])) if len(income_growth_arr) >= 3 else float(np.mean(income_growth_arr))

        raw_score = (real_rate * 3) - (income_growth * 2) + 50
        score = float(np.clip(raw_score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "real_interest_rate_pct": round(real_rate, 2),
            "income_growth_pct": round(income_growth, 2),
            "affordability_composite": round((real_rate * 3) - (income_growth * 2), 2),
            "n_rate_obs": len(rate_rows),
            "n_income_obs": len(income_rows),
            "methodology": "score = clip((real_rate * 3) - (income_growth * 2) + 50, 0, 100)",
        }
