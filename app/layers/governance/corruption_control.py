"""Corruption Control module.

Measures corruption perception level and trend using the World Bank WGI
Control of Corruption indicator (CC.EST).

CC.EST captures perceptions of the extent to which public power is exercised
for private gain, including both petty and grand forms of corruption, as well
as capture of the state by elites and private interests.

Score formula:
  base  = clip(50 - cc_latest * 20, 0, 100)
  If cc is declining (worsening corruption), the trend amplifies the score.
  Trend penalty = min(20, abs(slope) * 60) when slope < 0.

Sources: World Bank WDI (CC.EST)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class CorruptionControl(LayerBase):
    layer_id = "lGV"
    name = "Corruption Control"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'CC.EST'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 1:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]
        cc_latest = float(values[-1])

        base_score = float(np.clip(50.0 - cc_latest * 20.0, 0.0, 100.0))

        trend_penalty = 0.0
        slope = None
        if len(values) >= 3:
            x = np.arange(len(values), dtype=float)
            result = linregress(x, values)
            slope = float(result.slope)
            if slope < 0:
                # Declining CC (rising corruption) amplifies stress
                trend_penalty = min(20.0, abs(slope) * 60.0)

        score = float(np.clip(base_score + trend_penalty, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "cc_latest": round(cc_latest, 4),
            "n_obs": len(values),
            "period": f"{dates[0]} to {dates[-1]}",
            "trend_slope": round(slope, 6) if slope is not None else None,
            "trend_penalty": round(trend_penalty, 2),
            "worsening": slope is not None and slope < 0,
            "note": "CC.EST scale: -2.5 (high corruption) to +2.5 (low corruption)",
        }
