"""Eviction and Housing Stress module.

Eviction rate and housing insecurity index proxy. Uses unemployment rate
(SL.UEM.TOTL.ZS) as income-loss risk and poverty headcount (SI.POV.DDAY)
as structural insecurity signal. High unemployment combined with high
poverty amplifies eviction risk and housing insecurity.

Score = clip((unemployment_pct * 4) + (poverty_pct * 1.5), 0, 100)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class EvictionHousingStress(LayerBase):
    layer_id = "lHO"
    name = "Eviction and Housing Stress"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        unem_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.UEM.TOTL.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        poverty_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.POV.DDAY'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not unem_rows or len(unem_rows) < 1:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient unemployment data for eviction stress analysis",
            }

        if not poverty_rows or len(poverty_rows) < 1:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient poverty data for eviction stress analysis",
            }

        unem_vals = np.array([float(r["value"]) for r in unem_rows])
        poverty_vals = np.array([float(r["value"]) for r in poverty_rows])

        unemployment_pct = float(np.mean(unem_vals[-3:])) if len(unem_vals) >= 3 else float(unem_vals[-1])
        poverty_pct = float(poverty_vals[-1])

        raw_score = (unemployment_pct * 4) + (poverty_pct * 1.5)
        score = float(np.clip(raw_score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "unemployment_rate_pct": round(unemployment_pct, 2),
            "poverty_headcount_pct": round(poverty_pct, 2),
            "eviction_stress_composite": round((unemployment_pct * 4) + (poverty_pct * 1.5), 2),
            "n_unem_obs": len(unem_rows),
            "n_poverty_obs": len(poverty_rows),
            "methodology": "score = clip((unemployment * 4) + (poverty * 1.5), 0, 100)",
        }
