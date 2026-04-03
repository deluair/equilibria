"""Bureaucratic Quality module.

Uses Government Effectiveness (GE.EST) as a proxy for bureaucratic quality.

GE.EST captures perceptions of the quality of public services, the quality
of the civil service and the degree of its independence from political
pressures, the quality of policy formulation and implementation, and the
credibility of the government's commitment to such policies.

Score formula:
  score = clip(50 - ge_latest * 20, 0, 100)
  ge = +2.5 -> score = 0  (excellent bureaucratic quality)
  ge =  0.0 -> score = 50 (average quality)
  ge = -2.5 -> score = 100 (dysfunctional bureaucracy)

Sources: World Bank WDI (GE.EST)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class BureaucraticQuality(LayerBase):
    layer_id = "lGV"
    name = "Bureaucratic Quality"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GE.EST'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 1:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]
        ge_latest = float(values[-1])

        score = float(np.clip(50.0 - ge_latest * 20.0, 0.0, 100.0))

        slope = None
        if len(values) >= 3:
            x = np.arange(len(values), dtype=float)
            result = linregress(x, values)
            slope = float(result.slope)

        return {
            "score": round(score, 1),
            "country": country,
            "ge_latest": round(ge_latest, 4),
            "n_obs": len(values),
            "period": f"{dates[0]} to {dates[-1]}",
            "trend_slope": round(slope, 6) if slope is not None else None,
            "deteriorating": slope is not None and slope < 0,
            "note": "GE.EST scale: -2.5 (dysfunctional) to +2.5 (highly effective)",
        }
