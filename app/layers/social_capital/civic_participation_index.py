"""Civic Participation Index module.

Uses Voice and Accountability (VA.EST) from World Bank WGI as a proxy
for civic participation and democratic engagement.

Score formula:
  score = clip(50 - va_latest * 20, 0, 100)
  High VA.EST (good accountability) -> low stress score.

Sources: World Bank WDI (VA.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CivicParticipationIndex(LayerBase):
    layer_id = "lSC"
    name = "Civic Participation Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("VA.EST", "%Voice and Accountability%"),
        )

        if not rows:
            # Fallback: join query
            rows = await db.fetch_all(
                """
                SELECT dp.value FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ? AND ds.series_id = 'VA.EST'
                ORDER BY dp.date DESC LIMIT 15
                """,
                (country,),
            )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for VA.EST",
            }

        values = [float(r["value"]) for r in rows]
        va_latest = values[0]

        score = float(np.clip(50.0 - va_latest * 20.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "va_est_latest": round(va_latest, 4),
            "n_obs": len(values),
            "note": "VA.EST scale: -2.5 (worst) to +2.5 (best)",
        }
