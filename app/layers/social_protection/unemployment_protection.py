"""Unemployment Protection module.

Unemployment insurance gap: high unemployment combined with low social spending.

Queries:
- 'SL.UEM.TOTL.ZS' (unemployment rate, % of total labor force)
- 'GC.XPN.TOTL.GD.ZS' (general government total expenditure as % of GDP)

High unemployment + low government expenditure = protection gap.

Score = clip(unemployment_rate * max(0, 30 - govt_spend) / 10, 0, 100)

Sources: WDI (SL.UEM.TOTL.ZS, GC.XPN.TOTL.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class UnemploymentProtection(LayerBase):
    layer_id = "lSP"
    name = "Unemployment Protection"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        unem_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.UEM.TOTL.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        spend_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.XPN.TOTL.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not unem_rows or not spend_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        unem_vals = [float(r["value"]) for r in unem_rows if r["value"] is not None]
        spend_vals = [float(r["value"]) for r in spend_rows if r["value"] is not None]

        if not unem_vals or not spend_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        unemployment_rate = float(np.mean(unem_vals))
        govt_spend = float(np.mean(spend_vals))

        spend_gap = max(0.0, 30.0 - govt_spend)
        score = float(np.clip(unemployment_rate * spend_gap / 10.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "unemployment_rate_pct": round(unemployment_rate, 2),
            "govt_expenditure_pct_gdp": round(govt_spend, 2),
            "spend_gap": round(spend_gap, 2),
            "n_obs_unemployment": len(unem_vals),
            "n_obs_spend": len(spend_vals),
            "interpretation": (
                "High unemployment combined with low government expenditure "
                "signals an unemployment protection gap."
            ),
            "_series": ["SL.UEM.TOTL.ZS", "GC.XPN.TOTL.GD.ZS"],
            "_source": "WDI",
        }
