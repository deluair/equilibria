"""Volunteer Economy Size module.

Proxies civil society and volunteer sector strength using:
  VA.EST  - Voice and Accountability (World Bank WGI) as civic space proxy
  RL.EST  - Rule of Law as enabling environment for civil society

Higher governance scores = larger, more active volunteer/civil economy
= lower stress.

Score formula:
  composite = mean(VA.EST, RL.EST) on [-2.5, +2.5] scale
  score = clip(50 - composite * 20, 0, 100)

Sources: World Bank WDI (VA.EST, RL.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class VolunteerEconomySize(LayerBase):
    layer_id = "lSC"
    name = "Volunteer Economy Size"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('VA.EST', 'RL.EST')
            ORDER BY ds.series_id, dp.date
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for VA.EST or RL.EST",
            }

        latest: dict[str, float] = {}
        series_values: dict[str, list[float]] = {}
        for r in rows:
            series_values.setdefault(r["series_id"], []).append(float(r["value"]))
        for sid, vals in series_values.items():
            latest[sid] = vals[-1]

        if not latest:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        composite = float(np.mean(list(latest.values())))
        score = float(np.clip(50.0 - composite * 20.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "composite_wgi": round(composite, 4),
            "voice_accountability": round(latest.get("VA.EST", float("nan")), 4),
            "rule_of_law": round(latest.get("RL.EST", float("nan")), 4),
            "n_indicators": len(latest),
            "note": "High score = constrained civil society / volunteer economy",
        }
