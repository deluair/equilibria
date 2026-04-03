"""Pension Adequacy module.

Pension system adequacy: elderly population share x fiscal sustainability.

Queries:
- 'SP.POP.65UP.TO.ZS' (population aged 65+ as % of total)
- 'GC.DOD.TOTL.GD.ZS' (central government debt as % of GDP)

High elderly share combined with high public debt signals pension stress.

Score = clip(elderly_share * 2 + debt / 100 * 20, 0, 100)

Sources: WDI (SP.POP.65UP.TO.ZS, GC.DOD.TOTL.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PensionAdequacy(LayerBase):
    layer_id = "lSP"
    name = "Pension Adequacy"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        elderly_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.POP.65UP.TO.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        debt_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.DOD.TOTL.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not elderly_rows or not debt_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        elderly_vals = [float(r["value"]) for r in elderly_rows if r["value"] is not None]
        debt_vals = [float(r["value"]) for r in debt_rows if r["value"] is not None]

        if not elderly_vals or not debt_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        elderly_share = float(np.mean(elderly_vals))
        debt = float(np.mean(debt_vals))

        score = float(np.clip(elderly_share * 2.0 + debt / 100.0 * 20.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "elderly_share_pct": round(elderly_share, 2),
            "debt_pct_gdp": round(debt, 2),
            "n_obs_elderly": len(elderly_vals),
            "n_obs_debt": len(debt_vals),
            "interpretation": (
                "High elderly population share combined with elevated public debt "
                "signals fiscal stress on pension systems."
            ),
            "_series": ["SP.POP.65UP.TO.ZS", "GC.DOD.TOTL.GD.ZS"],
            "_source": "WDI",
        }
