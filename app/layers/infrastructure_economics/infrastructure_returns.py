"""Infrastructure Returns module.

Estimates the implied GDP multiplier from infrastructure spending by examining
the relationship between gross fixed capital formation (public proxy) and GDP
growth over a rolling window.

Sources: WDI NE.GDI.FTOT.ZS (GFCF % of GDP), WDI NY.GDP.MKTP.KD.ZG (GDP growth, annual %).
Score = clip(100 - estimated_multiplier * 20, 0, 100).
Higher multiplier -> lower stress score (better returns).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

WINDOW = 10  # years for rolling regression


class InfrastructureReturns(LayerBase):
    layer_id = "lIF"
    name = "Infrastructure Returns"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        gfcf_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.GDI.FTOT.ZS'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        gdp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        if len(gfcf_rows) < 3 or len(gdp_rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        gfcf_by_date = {str(r["date"]): float(r["value"]) for r in gfcf_rows}
        gdp_by_date = {str(r["date"]): float(r["value"]) for r in gdp_rows}
        common_dates = sorted(set(gfcf_by_date) & set(gdp_by_date))

        if len(common_dates) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient overlapping data"}

        x = np.array([gfcf_by_date[d] for d in common_dates])
        y = np.array([gdp_by_date[d] for d in common_dates])
        # Simple OLS slope as implied multiplier proxy
        x_dm = x - x.mean()
        denom = np.dot(x_dm, x_dm)
        multiplier = float(np.dot(x_dm, y - y.mean()) / denom) if denom > 0 else 0.0

        # Score: low/negative multiplier -> high stress; multiplier ~1.5 -> near zero stress
        score = float(np.clip(100.0 - multiplier * 33.3, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "estimated_gdp_multiplier": round(multiplier, 3),
            "observations": len(common_dates),
            "interpretation": (
                "Weak or negative infrastructure returns: spending not translating to growth"
                if multiplier < 0.5
                else "Moderate infrastructure multiplier" if multiplier < 1.0
                else "Healthy infrastructure GDP returns" if multiplier < 1.5
                else "High infrastructure multiplier: strong returns"
            ),
            "_sources": ["WDI:NE.GDI.FTOT.ZS", "WDI:NY.GDP.MKTP.KD.ZG"],
            "_note": "Multiplier estimated via OLS of GDP growth on GFCF share; proxy only.",
        }
