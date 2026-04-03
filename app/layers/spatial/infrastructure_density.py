"""Infrastructure density: road network density as spatial infrastructure proxy.

Road network density (km of roads per km² of land area) is a primary indicator
of spatial infrastructure capacity. Low road density signals infrastructure gaps
that constrain economic integration, market access, and spatial mobility.

Score = clip(max(0, 0.5 - road_density) * 200, 0, 100)
At road_density = 0.5 km/km²: score = 0 (adequate)
At road_density = 0.0 km/km²: score = 100 (maximum stress)

References:
    Calderón, C. & Servén, L. (2008). Infrastructure and Economic Development
        in Sub-Saharan Africa. World Bank Policy Research WP 4712.
    Donaldson, D. (2018). Railroads of the Raj. American Economic Review, 108(4-5).

Sources: World Bank WDI IS.ROD.TOTL.KM, AG.LND.TOTL.K2.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class InfrastructureDensity(LayerBase):
    layer_id = "l11"
    name = "Infrastructure Density"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        road_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IS.ROD.TOTL.KM'
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

        if not road_rows or not land_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no road or land area data",
                "country": country,
            }

        road_km = float(road_rows[0]["value"])
        land_km2 = float(land_rows[0]["value"])

        if land_km2 <= 0:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "invalid land area",
                "country": country,
            }

        road_density = road_km / land_km2  # km road per km² land

        score = float(np.clip(max(0.0, 0.5 - road_density) * 200.0, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "road_network_km": round(road_km, 0),
            "land_area_km2": round(land_km2, 0),
            "road_density_km_per_km2": round(road_density, 4),
            "road_year": road_rows[0]["date"],
            "infrastructure_gap": (
                "severe" if road_density < 0.1
                else "high" if road_density < 0.25
                else "moderate" if road_density < 0.5
                else "adequate"
            ),
            "_source": "WDI IS.ROD.TOTL.KM, AG.LND.TOTL.K2",
        }
