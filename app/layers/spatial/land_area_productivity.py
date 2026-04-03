"""Land area productivity: economic density (GDP per km²).

Economic density measures how productively a country uses its land area.
Low GDP per km² relative to income level indicates spatial inefficiency,
underdeveloped economic geography, or poor spatial organization.

Score reflects the gap between actual and benchmark density:
- Compute GDP per km² from GDP (constant USD) and land area (km²).
- Score based on log-scale: very low density = high stress.
- log10(gdp_per_km2) scaled: < 2 = severe (score ~90), > 6 = low (score ~5).

Score = max(0, 100 - (log10(gdp_per_km2) - 2) * 25), clipped 0-100.

References:
    Ciccone, A. & Hall, R.E. (1996). Productivity and the Density of Economic
        Activity. American Economic Review, 86(1), 54-70.
    World Bank (2009). World Development Report: Reshaping Economic Geography.

Sources: World Bank WDI NY.GDP.MKTP.KD, AG.LND.TOTL.K2.
"""

from __future__ import annotations

import math

import numpy as np

from app.layers.base import LayerBase


class LandAreaProductivity(LayerBase):
    layer_id = "l11"
    name = "Land Area Productivity"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        gdp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.MKTP.KD'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        land_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'AG.LND.TOTL.K2'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not gdp_rows or not land_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient GDP or land area data",
                "country": country,
            }

        gdp = float(gdp_rows[0]["value"])
        land_km2 = float(land_rows[0]["value"])

        if land_km2 <= 0 or gdp <= 0:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "zero or negative GDP or land area",
                "country": country,
            }

        gdp_per_km2 = gdp / land_km2
        log_density = math.log10(gdp_per_km2)

        score = float(np.clip(100.0 - (log_density - 2.0) * 25.0, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "gdp_constant_usd": round(gdp, 0),
            "land_area_km2": round(land_km2, 0),
            "gdp_per_km2": round(gdp_per_km2, 2),
            "log10_gdp_per_km2": round(log_density, 4),
            "gdp_year": gdp_rows[0]["date"],
            "density_level": (
                "very_high" if gdp_per_km2 > 1e6
                else "high" if gdp_per_km2 > 1e5
                else "moderate" if gdp_per_km2 > 1e4
                else "low" if gdp_per_km2 > 1e3
                else "very_low"
            ),
            "_source": "WDI NY.GDP.MKTP.KD, AG.LND.TOTL.K2",
        }
