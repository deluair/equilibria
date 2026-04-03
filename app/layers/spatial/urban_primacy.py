"""Urban primacy index: concentration of urban population in the largest city.

High urban primacy indicates over-concentration of economic activity, infrastructure,
and population in a single city, which can reduce national economic efficiency and
create spatial inequality between the primate city and secondary cities.

Urban primacy index = share of urban population in the largest city (%).
Benchmark: >50% = over-concentrated; 25-50% = moderate; <25% = polycentric.

Score = clip((primacy - 25) * 2, 0, 100)
At 25%: score = 0 (no stress); at 75%: score = 100 (maximum stress).

References:
    Jefferson, M. (1939). The Law of the Primate City. Geographical Review, 29(2).
    Henderson, J.V. (2003). The Urbanization Process and Economic Growth.
        Journal of Economic Growth, 8(1), 47-71.

Sources: World Bank WDI EN.URB.MCTY.TL.ZS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class UrbanPrimacy(LayerBase):
    layer_id = "l11"
    name = "Urban Primacy"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'EN.URB.MCTY.TL.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no urban primacy data",
                "country": country,
            }

        latest = rows[0]
        primacy = float(latest["value"])
        year = latest["date"]

        # Trend over available observations
        trend_slope = None
        if len(rows) >= 3:
            vals = np.array([float(r["value"]) for r in reversed(rows)])
            t = np.arange(len(vals), dtype=float)
            slope = float(np.polyfit(t, vals, 1)[0])
            trend_slope = round(slope, 4)

        score = float(np.clip((primacy - 25.0) * 2.0, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "primacy_pct": round(primacy, 2),
            "year": year,
            "trend_slope_pp_per_yr": trend_slope,
            "concentration": (
                "high" if primacy > 50 else "moderate" if primacy > 25 else "low"
            ),
            "n_obs": len(rows),
            "_source": "WDI EN.URB.MCTY.TL.ZS",
        }
