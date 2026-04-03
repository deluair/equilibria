"""Pension Fiscal Sustainability module.

Measures pension system fiscal sustainability by combining elderly population
share with general government debt levels. High elderly share and high debt
together indicate an unsustainable pension financing path.

Score = clip(elderly_share * debt_gdp / 50, 0, 100)

Sources: WDI SP.POP.65UP.TO.ZS (elderly % of total population),
         WDI GC.DOD.TOTL.GD.ZS (central government debt % of GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PensionFiscalSustainability(LayerBase):
    layer_id = "lPS"
    name = "Pension Fiscal Sustainability"

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

        if not elderly_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no elderly population data"}
        if not debt_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no debt data"}

        elderly_vals = [float(r["value"]) for r in elderly_rows if r["value"] is not None]
        debt_vals = [float(r["value"]) for r in debt_rows if r["value"] is not None]

        if not elderly_vals or not debt_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid data"}

        elderly_share = float(np.mean(elderly_vals))
        debt_gdp = float(np.mean(debt_vals))

        score = float(np.clip(elderly_share * debt_gdp / 50.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "elderly_share_pct": round(elderly_share, 2),
            "debt_gdp_pct": round(debt_gdp, 2),
            "sustainability_ratio": round(elderly_share * debt_gdp / 50.0, 3),
            "unsustainable": score > 75,
            "interpretation": (
                "unsustainable pension financing" if score > 75
                else "at-risk pension financing" if score > 50
                else "moderate fiscal pressure" if score > 25
                else "manageable pension fiscal burden"
            ),
            "sources": ["WDI SP.POP.65UP.TO.ZS", "WDI GC.DOD.TOTL.GD.ZS"],
        }
