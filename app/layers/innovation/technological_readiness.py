"""Technological Readiness module.

Assesses a country's readiness to adopt and leverage technology using:
- Internet users as % of population (IT.NET.USER.ZS)
- Mobile cellular subscriptions per 100 people (IT.CEL.SETS.P2)

Score = max(0, 100 - (internet + mobile/10) / 2)

High internet penetration and mobile density lower the score (better readiness).
Low penetration signals digital infrastructure gaps and technology adoption barriers.

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TechnologicalReadiness(LayerBase):
    layer_id = "lNV"
    name = "Technological Readiness"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        internet_rows = await db.fetch_all(
            """
            SELECT dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IT.NET.USER.ZS'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        mobile_rows = await db.fetch_all(
            """
            SELECT dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IT.CEL.SETS.P2'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        internet: float | None = None
        mobile: float | None = None

        if internet_rows:
            vals = [float(r["value"]) for r in internet_rows if r["value"] is not None]
            internet = float(np.mean(vals)) if vals else None

        if mobile_rows:
            vals = [float(r["value"]) for r in mobile_rows if r["value"] is not None]
            mobile = float(np.mean(vals)) if vals else None

        if internet is None and mobile is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        # Use available dimensions; fall back gracefully
        if internet is not None and mobile is not None:
            score = max(0.0, 100.0 - (internet + mobile / 10.0) / 2.0)
        elif internet is not None:
            score = max(0.0, 100.0 - internet)
        else:
            score = max(0.0, 100.0 - mobile / 10.0)

        score = min(100.0, score)

        return {
            "score": round(score, 1),
            "country": country,
            "internet_pct": round(internet, 2) if internet is not None else None,
            "mobile_per_100": round(mobile, 2) if mobile is not None else None,
            "interpretation": (
                "High score = low technological readiness; "
                "low score = high connectivity and adoption"
            ),
        }
