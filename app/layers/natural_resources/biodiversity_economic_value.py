"""Biodiversity economic value: ecosystem service value at risk.

Proxies ecosystem service value and biodiversity-related economic risk using:
  AG.LND.FRST.ZS    - forest area (% of land) [habitat proxy]
  AG.LND.PRCP.MM    - average precipitation (mm/year) [ecosystem productivity]
  EN.BIR.THRD.NO    - bird species (threatened) [biodiversity stress indicator]
  NY.GDP.TOTL.RT.ZS - resource rents (% GDP) [extraction pressure]

Score (higher = greater ecosystem value at risk):
  s_threatened  = clip(threatened_birds * 0.5, 0, 40) [direct biodiversity loss]
  s_habitat     = clip((60 - forest_pct) / 60 * 30, 0, 30) [habitat loss pressure]
  s_extraction  = clip(rent_pct * 1.0, 0, 30) [resource extraction pressure]
  score = clip(s_threatened + s_habitat + s_extraction, 0, 100)

Sources: World Bank WDI (AG.LND.FRST.ZS, EN.BIR.THRD.NO, NY.GDP.TOTL.RT.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class BiodiversityEconomicValue(LayerBase):
    layer_id = "lNR"
    name = "Biodiversity Economic Value"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id IN (
                'AG.LND.FRST.ZS', 'EN.BIR.THRD.NO',
                'NY.GDP.TOTL.RT.ZS', 'AG.LND.PRCP.MM'
            )
              AND ds.country_iso3 = ?
            ORDER BY dp.date DESC
            LIMIT 40
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no biodiversity or ecosystem data",
            }

        latest: dict[str, tuple[str, float]] = {}
        for r in rows:
            sid = r["series_id"]
            if sid not in latest and r["value"] is not None:
                latest[sid] = (r["date"][:4], float(r["value"]))

        if not latest:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "all biodiversity values are null",
            }

        # Threatened birds sub-score (0-40)
        s_threatened = 0.0
        threatened_birds = None
        bird_data = latest.get("EN.BIR.THRD.NO")
        if bird_data:
            threatened_birds = bird_data[1]
            s_threatened = float(np.clip(threatened_birds * 0.5, 0, 40))

        # Habitat loss sub-score (0-30): low forest = high risk
        s_habitat = 0.0
        forest_pct = None
        forest_data = latest.get("AG.LND.FRST.ZS")
        if forest_data:
            forest_pct = forest_data[1]
            s_habitat = float(np.clip((60.0 - forest_pct) / 60.0 * 30.0, 0, 30))

        # Extraction pressure sub-score (0-30)
        s_extraction = 0.0
        rent_pct = None
        rent_data = latest.get("NY.GDP.TOTL.RT.ZS")
        if rent_data:
            rent_pct = rent_data[1]
            s_extraction = float(np.clip(rent_pct * 1.0, 0, 30))

        score = float(np.clip(s_threatened + s_habitat + s_extraction, 0, 100))

        precipitation_mm = None
        prcp_data = latest.get("AG.LND.PRCP.MM")
        if prcp_data:
            precipitation_mm = prcp_data[1]

        biodiversity_risk = (
            "critical" if score >= 70
            else "high" if score >= 50
            else "moderate" if score >= 30
            else "low"
        )

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": country,
                "threatened_bird_species": (
                    int(threatened_birds) if threatened_birds is not None else None
                ),
                "forest_area_pct_land": round(forest_pct, 3) if forest_pct is not None else None,
                "resource_rent_pct_gdp": round(rent_pct, 3) if rent_pct is not None else None,
                "avg_precipitation_mm": (
                    round(precipitation_mm, 1) if precipitation_mm is not None else None
                ),
                "sub_scores": {
                    "threatened_species": round(s_threatened, 2),
                    "habitat_loss": round(s_habitat, 2),
                    "extraction_pressure": round(s_extraction, 2),
                },
                "biodiversity_risk": biodiversity_risk,
                "indicators_available": len(latest),
            },
        }
