"""Urban Land Use module.

Measures urban concentration and land efficiency. Queries SP.URB.TOTL.IN.ZS
(urban population %). Very high urbanization (>80%) with low income suggests
overurbanization stress; very low urbanization indicates underurbanization.
Score captures deviation from a 55% benchmark.

Score = clip(abs(urban_pct - 55) * 1.2, 0, 100)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class UrbanLandUse(LayerBase):
    layer_id = "lRE"
    name = "Urban Land Use"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        urban_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.URB.TOTL.IN.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        gdp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not urban_rows or len(urban_rows) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient urban population data for land use analysis",
            }

        urban_vals = np.array([float(r["value"]) for r in urban_rows])
        urban_pct = float(urban_vals[-1])

        # Base score: deviation from 55% benchmark
        raw_score = abs(urban_pct - 55.0) * 1.2

        # Amplify stress if highly urbanized with low income (overurbanization)
        gdp_penalty = 0.0
        gdp_latest = None
        if gdp_rows and len(gdp_rows) >= 1:
            gdp_vals = np.array([float(r["value"]) for r in gdp_rows])
            gdp_latest = float(gdp_vals[-1])
            if urban_pct > 80 and gdp_latest < 3000:
                gdp_penalty = min(20.0, (80 - gdp_latest / 100) * 0.5)

        score = float(np.clip(raw_score + gdp_penalty, 0, 100))

        stress_type = (
            "OVERURBANIZED" if urban_pct > 80
            else "UNDERURBANIZED" if urban_pct < 30
            else "MODERATE"
        )

        return {
            "score": round(score, 1),
            "country": country,
            "urban_population_pct": round(urban_pct, 2),
            "benchmark_pct": 55.0,
            "deviation_from_benchmark": round(urban_pct - 55.0, 2),
            "gdp_per_capita_usd": round(gdp_latest, 0) if gdp_latest is not None else None,
            "stress_type": stress_type,
            "n_urban_obs": len(urban_rows),
            "methodology": "score = clip(abs(urban_pct - 55) * 1.2, 0, 100); amplified if >80% urban + low income",
        }
