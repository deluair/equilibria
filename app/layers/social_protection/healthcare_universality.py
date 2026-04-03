"""Healthcare Universality module.

Universal health coverage proxy: health spending and hospital bed availability.

Queries:
- 'SH.XPD.CHEX.GD.ZS' (current health expenditure as % of GDP)
- 'SH.MED.BEDS.ZS' (hospital beds per 1,000 people)

Low spending + low bed availability = universal coverage gap.

Score = clip(max(0, 6 - spend) * 10 + max(0, 3 - beds) * 10, 0, 100)

Sources: WDI (SH.XPD.CHEX.GD.ZS, SH.MED.BEDS.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HealthcareUniversality(LayerBase):
    layer_id = "lSP"
    name = "Healthcare Universality"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

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

        if not spend_rows or not beds_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        spend_vals = [float(r["value"]) for r in spend_rows if r["value"] is not None]
        beds_vals = [float(r["value"]) for r in beds_rows if r["value"] is not None]

        if not spend_vals or not beds_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        spend = float(np.mean(spend_vals))
        beds = float(np.mean(beds_vals))

        spend_penalty = max(0.0, 6.0 - spend) * 10.0
        beds_penalty = max(0.0, 3.0 - beds) * 10.0
        score = float(np.clip(spend_penalty + beds_penalty, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "health_spend_pct_gdp": round(spend, 2),
            "hospital_beds_per_1000": round(beds, 2),
            "spend_penalty": round(spend_penalty, 2),
            "beds_penalty": round(beds_penalty, 2),
            "n_obs_spend": len(spend_vals),
            "n_obs_beds": len(beds_vals),
            "interpretation": (
                "Low health expenditure and hospital bed density signal "
                "a gap in universal health coverage."
            ),
            "_series": ["SH.XPD.CHEX.GD.ZS", "SH.MED.BEDS.ZS"],
            "_source": "WDI",
        }
