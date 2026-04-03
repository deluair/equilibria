"""Pandemic Preparedness module.

Measures health system capacity as a proxy for pandemic readiness.
Low hospital beds (SH.MED.BEDS.ZS) and low health expenditure
(SH.XPD.CHEX.GD.ZS) signal high vulnerability.

Score = clip(max(0, 3 - beds) * 20 + max(0, 5 - spend) * 10, 0, 100).

Sources: WDI (SH.MED.BEDS.ZS, SH.XPD.CHEX.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PandemicPreparedness(LayerBase):
    layer_id = "lDE"
    name = "Pandemic Preparedness"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        beds_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SH.MED.BEDS.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        spend_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SH.XPD.CHEX.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not beds_rows and not spend_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        beds_values = [float(r["value"]) for r in beds_rows if r["value"] is not None]
        spend_values = [float(r["value"]) for r in spend_rows if r["value"] is not None]

        beds = float(np.mean(beds_values)) if beds_values else 3.0
        spend = float(np.mean(spend_values)) if spend_values else 5.0

        beds_penalty = max(0.0, 3.0 - beds) * 20.0
        spend_penalty = max(0.0, 5.0 - spend) * 10.0
        score = float(np.clip(beds_penalty + spend_penalty, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "hospital_beds_per_1000": round(beds, 4),
            "health_spend_pct_gdp": round(spend, 4),
            "beds_penalty": round(beds_penalty, 2),
            "spend_penalty": round(spend_penalty, 2),
            "indicators": {
                "beds": "SH.MED.BEDS.ZS",
                "health_spend": "SH.XPD.CHEX.GD.ZS",
            },
        }
