"""Conflict and Disaster Nexus module.

Measures compound fragility from co-occurring conflict and natural disaster risk.
Low political stability combined with high disaster exposure creates cascading
vulnerability that exceeds either risk alone.

Indicators:
  PV.EST        -- Political stability and absence of violence estimate
  EN.CLC.MDAT.ZS -- Population affected by droughts, floods, extreme temps (%)

Score = clip((50 - pv*20)/2 + disaster_exposure/2, 0, 100)

Sources: WDI (PV.EST, EN.CLC.MDAT.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ConflictDisasterNexus(LayerBase):
    layer_id = "lDE"
    name = "Conflict Disaster Nexus"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        async def _fetch(series_id: str) -> list[float]:
            rows = await db.fetch_all(
                """
                SELECT dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT 10
                """,
                (country, series_id),
            )
            return [float(r["value"]) for r in rows if r["value"] is not None]

        pv_vals = await _fetch("PV.EST")
        disaster_vals = await _fetch("EN.CLC.MDAT.ZS")

        if not pv_vals and not disaster_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        pv = float(np.mean(pv_vals)) if pv_vals else 0.0
        disaster_exposure = float(np.mean(disaster_vals)) if disaster_vals else 25.0

        # PV.EST range: approx -2.5 to 2.5; lower = more fragile
        # At pv=0: (50-0)/2 = 25; at pv=-2.5: (50+50)/2 = 50
        stability_component = (50.0 - pv * 20.0) / 2.0
        disaster_component = disaster_exposure / 2.0
        score = float(np.clip(stability_component + disaster_component, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "political_stability_est": round(pv, 4),
            "disaster_exposure_pct": round(disaster_exposure, 4),
            "stability_component": round(stability_component, 2),
            "disaster_component": round(disaster_component, 2),
            "indicators": {
                "political_stability": "PV.EST",
                "disaster_exposure": "EN.CLC.MDAT.ZS",
            },
        }
