"""Government Accountability module.

Measures voice and accountability using the World Bank WGI indicator VA.EST.

VA.EST captures perceptions of the extent to which a country's citizens are
able to participate in selecting their government, as well as freedom of
expression, freedom of association, and a free media.

Score formula:
  score = clip(50 - va_latest * 20, 0, 100)
  va = +2.5 -> score = 0  (strong accountability, no stress)
  va =  0.0 -> score = 50 (average accountability)
  va = -2.5 -> score = 100 (authoritarian, crisis)

Sources: World Bank WDI (VA.EST)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class GovernmentAccountability(LayerBase):
    layer_id = "lGV"
    name = "Government Accountability"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'VA.EST'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 1:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]
        va_latest = float(values[-1])

        score = float(np.clip(50.0 - va_latest * 20.0, 0.0, 100.0))

        slope = None
        if len(values) >= 3:
            x = np.arange(len(values), dtype=float)
            result = linregress(x, values)
            slope = float(result.slope)

        return {
            "score": round(score, 1),
            "country": country,
            "va_latest": round(va_latest, 4),
            "n_obs": len(values),
            "period": f"{dates[0]} to {dates[-1]}",
            "trend_slope": round(slope, 6) if slope is not None else None,
            "eroding": slope is not None and slope < 0,
            "note": "VA.EST scale: -2.5 (no accountability) to +2.5 (strong accountability)",
        }
