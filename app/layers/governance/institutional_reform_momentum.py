"""Institutional Reform Momentum module.

Measures the trend in overall governance quality over the available history
using all 6 World Bank WGI dimensions.

Method:
  1. Query all 6 WGI indicators: VA.EST, PV.EST, GE.EST, RQ.EST, RL.EST, CC.EST.
  2. For each year, compute the average across available indicators (composite).
  3. Run a linear regression (linregress) on the composite time series.
  4. The slope captures reform momentum:
     Positive slope = improving institutions = low reform-reversal risk.
     Negative slope = deteriorating institutions = high stress.

Score formula:
  If slope > 0 (improving): score = max(0, 25 - slope * 50)
    (strong improvement = score close to 0)
  If slope <= 0 (declining): score = clip(50 + abs(slope) * 100, 0, 100)
    (steep decline = crisis-level score)
  Minimum data requirement: 3 years with composite values.

Sources: World Bank WDI (VA.EST, PV.EST, GE.EST, RQ.EST, RL.EST, CC.EST)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase

WGI_INDICATORS = ["VA.EST", "PV.EST", "GE.EST", "RQ.EST", "RL.EST", "CC.EST"]


class InstitutionalReformMomentum(LayerBase):
    layer_id = "lGV"
    name = "Institutional Reform Momentum"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('VA.EST','PV.EST','GE.EST','RQ.EST','RL.EST','CC.EST')
            ORDER BY dp.date, ds.series_id
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        # Build year -> values map
        year_values: dict[str, list[float]] = {}
        for r in rows:
            year = r["date"][:4]
            year_values.setdefault(year, []).append(float(r["value"]))

        # Compute composite per year (mean of available indicators)
        sorted_years = sorted(year_values.keys())
        composites = [float(np.mean(year_values[y])) for y in sorted_years]

        if len(composites) < 3:
            # Insufficient history for trend
            latest_composite = composites[-1] if composites else 0.0
            score = float(np.clip(50.0 - latest_composite * 20.0, 0.0, 100.0))
            return {
                "score": round(score, 1),
                "country": country,
                "n_years": len(composites),
                "slope": None,
                "signal": "WATCH",
                "note": "fewer than 3 years of data; trend unavailable",
            }

        x = np.arange(len(composites), dtype=float)
        y = np.array(composites)
        result = linregress(x, y)
        slope = float(result.slope)
        r2 = float(result.rvalue ** 2)

        if slope > 0:
            score = float(np.clip(25.0 - slope * 50.0, 0.0, 100.0))
        else:
            score = float(np.clip(50.0 + abs(slope) * 100.0, 0.0, 100.0))

        latest_composite = composites[-1]

        return {
            "score": round(score, 1),
            "country": country,
            "wgi_composite_latest": round(latest_composite, 4),
            "slope_per_year": round(slope, 6),
            "r_squared": round(r2, 4),
            "n_years": len(composites),
            "period": f"{sorted_years[0]} to {sorted_years[-1]}",
            "direction": "improving" if slope > 0 else "declining",
            "reform_reversal": slope < 0,
            "indicators_queried": WGI_INDICATORS,
        }
