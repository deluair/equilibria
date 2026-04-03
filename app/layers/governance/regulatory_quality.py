"""Regulatory Quality module.

Measures the quality of the regulatory environment using the World Bank WGI
Regulatory Quality indicator (RQ.EST).

RQ.EST captures perceptions of the government's ability to formulate and
implement sound policies and regulations that permit and promote private
sector development.

Score formula:
  score = clip(50 - rq_latest * 20, 0, 100)
  rq = +2.5 -> score = 0  (excellent regulatory environment)
  rq =  0.0 -> score = 50 (average)
  rq = -2.5 -> score = 100 (severe regulatory dysfunction)

Sources: World Bank WDI (RQ.EST)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class RegulatoryQuality(LayerBase):
    layer_id = "lGV"
    name = "Regulatory Quality"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'RQ.EST'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 1:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]
        rq_latest = float(values[-1])

        score = float(np.clip(50.0 - rq_latest * 20.0, 0.0, 100.0))

        slope = None
        if len(values) >= 3:
            x = np.arange(len(values), dtype=float)
            result = linregress(x, values)
            slope = float(result.slope)

        return {
            "score": round(score, 1),
            "country": country,
            "rq_latest": round(rq_latest, 4),
            "n_obs": len(values),
            "period": f"{dates[0]} to {dates[-1]}",
            "trend_slope": round(slope, 6) if slope is not None else None,
            "improving": slope is not None and slope > 0,
            "note": "RQ.EST scale: -2.5 (worst regulatory quality) to +2.5 (best)",
        }
