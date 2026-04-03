"""Population density stress: density vs agricultural land resource proxy.

High population density alone does not imply stress; Hong Kong and Singapore
are dense but wealthy. The stress signal emerges when density is high relative
to agricultural land availability, creating pressure on domestic food production,
water resources, and land allocation (FAO 2022, Boserup 1965 tension model).

Resource stress index combines:
  1. Density score: penalty rises above 100 persons/km2, steeply above 500.
  2. Agricultural land score: penalty when arable share < 20%, i.e. thin domestic
     food production buffer relative to population.
  3. Interaction: high density + low agricultural land = compounded stress.

Composite = 0.5 * density_score + 0.3 * agland_score + 0.2 * interaction_score.

References:
    Boserup, E. (1965). The Conditions of Agricultural Growth. Aldine.
    FAO (2022). The State of Food and Agriculture 2022.
    Headey, D. & Fan, S. (2010). Reflections on the Global Food Crisis.
        IFPRI Research Monograph 165.

Series:
    EN.POP.DNST  -- population density (people per sq km of land area)
    AG.LND.AGRI.ZS -- agricultural land (% of land area)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PopulationDensityStress(LayerBase):
    layer_id = "l17"
    name = "Population Density Stress"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3")

        if not country_iso3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "country_iso3 required",
            }

        density_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'EN.POP.DNST'
              AND ds.country_iso3 = ?
              AND dp.value IS NOT NULL
            ORDER BY dp.date
            """,
            (country_iso3,),
        )

        agland_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'AG.LND.AGRI.ZS'
              AND ds.country_iso3 = ?
              AND dp.value IS NOT NULL
            ORDER BY dp.date
            """,
            (country_iso3,),
        )

        if not density_rows and not agland_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"no density or agricultural land data for {country_iso3}",
            }

        # Latest values
        density = None
        density_year = None
        if density_rows:
            latest = density_rows[-1]
            density = float(latest["value"])
            density_year = latest["date"][:4]

        agland = None
        agland_year = None
        if agland_rows:
            latest = agland_rows[-1]
            agland = float(latest["value"])
            agland_year = latest["date"][:4]

        # --- Density score ---
        # Penalty rises above 100 p/km2, steeply above 500
        density_score = 0.0
        if density is not None:
            if density <= 50:
                density_score = density * 0.2          # 0-10
            elif density <= 100:
                density_score = 10 + (density - 50) * 0.4   # 10-30
            elif density <= 250:
                density_score = 30 + (density - 100) * 0.267  # 30-70
            elif density <= 500:
                density_score = 70 + (density - 250) * 0.08   # 70-90
            else:
                density_score = 90 + (density - 500) * 0.02   # 90+
        density_score = float(np.clip(density_score, 0, 100))

        # --- Agricultural land score ---
        # Stress when agricultural share < 20% (thin food buffer)
        agland_score = 0.0
        if agland is not None:
            if agland >= 40:
                agland_score = 0.0
            elif agland >= 20:
                agland_score = (40 - agland) * 1.5    # 0-30
            elif agland >= 10:
                agland_score = 30 + (20 - agland) * 3.0  # 30-60
            else:
                agland_score = 60 + (10 - agland) * 4.0  # 60+
        agland_score = float(np.clip(agland_score, 0, 100))

        # --- Interaction score ---
        # Compound stress: high density AND low agricultural land
        interaction_score = 0.0
        if density is not None and agland is not None:
            # Normalized: density above 200 and agland below 30 both contribute
            dens_factor = float(np.clip((density - 200) / 800, 0, 1))
            agland_factor = float(np.clip((30 - agland) / 30, 0, 1))
            interaction_score = float(np.clip(dens_factor * agland_factor * 100, 0, 100))

        # Composite
        if density is not None and agland is not None:
            score = 0.5 * density_score + 0.3 * agland_score + 0.2 * interaction_score
        elif density is not None:
            score = density_score
        else:
            score = agland_score

        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": country_iso3,
                "population_density_per_km2": round(density, 2) if density is not None else None,
                "density_year": density_year,
                "agricultural_land_pct": round(agland, 2) if agland is not None else None,
                "agland_year": agland_year,
                "density_score": round(density_score, 2),
                "agland_score": round(agland_score, 2),
                "interaction_score": round(interaction_score, 2),
                "stress_profile": (
                    "high-density-low-agland" if density is not None and agland is not None
                        and density > 200 and agland < 20
                    else "high-density" if density is not None and density > 500
                    else "low-agland" if agland is not None and agland < 10
                    else "moderate"
                ),
            },
        }
