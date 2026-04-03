"""Startup Density module.

Measures new firm registrations per 1,000 working-age population (15-64).
Uses World Bank WDI:
- IC.BUS.NDNS.ZS: New business density (new registrations per 1,000 people aged 15-64)

High density indicates a vibrant entrepreneurial environment. Low density
signals barriers to entry, weak entrepreneurial culture, or regulatory friction.

Score: higher score = lower startup density = more stress.

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class StartupDensity(LayerBase):
    layer_id = "lER"
    name = "Startup Density"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IC.BUS.NDNS.ZS'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for IC.BUS.NDNS.ZS"}

        vals = [float(row["value"]) for row in rows if row["value"] is not None]
        if not vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid values"}

        density = float(np.mean(vals))
        latest = float(rows[0]["value"])
        latest_year = rows[0]["date"][:4] if rows[0]["date"] else None

        # Normalize: 0-10 registrations per 1,000 is the global range.
        # Low density = high stress score.
        norm = min(100.0, (density / 10.0) * 100.0)
        score = max(0.0, 100.0 - norm)

        return {
            "score": round(score, 1),
            "country": country,
            "startup_density_per_1000": round(density, 4),
            "latest_value": round(latest, 4),
            "latest_year": latest_year,
            "interpretation": "High score = low startup density = stressed entrepreneurial environment",
        }
