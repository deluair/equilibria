"""Congestion Costs module.

Proxies urban congestion stress from high population density combined with
low road network coverage. High density with sparse roads implies chronic
congestion and elevated transport costs.

Sources: WDI EN.POP.DNST (population density, people per sq. km of land area),
         WDI IS.ROD.TOTL.KM (road network, total km).
Road density = IS.ROD.TOTL.KM normalized by AG.LND.TOTL.K2 (land area).
Score = clip((density / 1000) * (1 - road_density_index) * 100, 0, 100).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# Reference road density (km per sq km) typical of well-connected mid-income countries
_ROAD_DENSITY_REF = 0.5


class CongestionCosts(LayerBase):
    layer_id = "lUE"
    name = "Congestion Costs"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        density_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'EN.POP.DNST'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

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

        if not density_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no population density data"}

        pop_density = float(density_rows[0]["value"])

        road_density_km_per_km2 = None
        if road_rows and land_rows:
            road_km = float(road_rows[0]["value"])
            land_km2 = float(land_rows[0]["value"])
            if land_km2 > 0:
                road_density_km_per_km2 = road_km / land_km2

        # Road adequacy index: 1 = well-connected, 0 = no roads
        if road_density_km_per_km2 is not None:
            road_adequacy = float(np.clip(road_density_km_per_km2 / _ROAD_DENSITY_REF, 0, 1))
        else:
            # No road data: assume moderate adequacy as fallback
            road_adequacy = 0.5

        # Congestion stress: density pushes up, road adequacy buffers it
        density_factor = float(np.clip(pop_density / 1000.0, 0, 2))
        score = float(np.clip(density_factor * (1.0 - road_adequacy) * 100.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "population_density_per_km2": round(pop_density, 1),
            "road_density_km_per_km2": round(road_density_km_per_km2, 4)
            if road_density_km_per_km2 is not None
            else None,
            "road_adequacy_index": round(road_adequacy, 3),
            "interpretation": (
                "Severe congestion stress: very high density with inadequate road network"
                if score > 60
                else "Significant congestion pressure" if score > 35
                else "Moderate congestion risk" if score > 15
                else "Low congestion stress"
            ),
            "_sources": ["WDI:EN.POP.DNST", "WDI:IS.ROD.TOTL.KM", "WDI:AG.LND.TOTL.K2"],
        }
