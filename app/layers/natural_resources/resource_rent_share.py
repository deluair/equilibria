"""Resource rent share: natural resource rents as % of GDP.

Queries World Bank WDI series NY.GDP.TOTL.RT.ZS (total natural resources rents
as % of GDP). High rent share signals resource dependency and exposure to
commodity price volatility. Includes oil, natural gas, coal, mineral, and
forest rents.

Score = clip(rent_pct * 2.0, 0, 100):
  - rent_pct < 5%   -> low dependency (score < 10)
  - rent_pct = 15%  -> moderate dependency (score 30)
  - rent_pct >= 50% -> extreme rent dependence (score capped at 100)

Sources: World Bank WDI (NY.GDP.TOTL.RT.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ResourceRentShare(LayerBase):
    layer_id = "lNR"
    name = "Resource Rent Share"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.TOTL.RT.ZS'
              AND ds.country_iso3 = ?
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no natural resource rent data",
            }

        valid = [(r["date"][:4], float(r["value"])) for r in rows if r["value"] is not None]
        if not valid:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "all rent values are null",
            }

        latest_year, rent_pct = valid[0]
        score = float(np.clip(rent_pct * 2.0, 0, 100))

        dependency_level = (
            "extreme" if rent_pct >= 50
            else "high" if rent_pct >= 20
            else "moderate" if rent_pct >= 5
            else "low"
        )

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": country,
                "series_id": "NY.GDP.TOTL.RT.ZS",
                "latest_year": latest_year,
                "rent_pct_gdp": round(rent_pct, 3),
                "dependency_level": dependency_level,
            },
        }
