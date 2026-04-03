"""Renewable readiness: renewable electricity share vs peer benchmark.

Queries World Bank WDI series EG.ELC.RNEW.ZS (renewable electricity output
as % of total electricity). Measures how far below the 20% readiness threshold
a country sits. Countries below 20% are considered low-readiness for the
energy transition.

Score = max(0, 50 - renewables_pct):
  - 0% renewables  -> score 50
  - 20% renewables -> score 30
  - 50% renewables -> score 0
  - >= 50%         -> score 0

Sources: World Bank WDI (EG.ELC.RNEW.ZS)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class RenewableReadiness(LayerBase):
    layer_id = "l16"
    name = "Renewable Readiness"
    weight = 0.20

    # Peer benchmark: below this % = low readiness
    READINESS_THRESHOLD = 20.0

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3")

        if not country:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "country_iso3 required",
            }

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'EG.ELC.RNEW.ZS'
              AND ds.country_iso3 = ?
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no renewable electricity data",
            }

        valid = [(r["date"][:4], float(r["value"])) for r in rows if r["value"] is not None]

        if not valid:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "all renewable values are null",
            }

        latest_year, renewables_pct = valid[-1]

        score = float(np.clip(max(0.0, 50.0 - renewables_pct), 0, 100))

        trend = None
        if len(valid) >= 5:
            yrs = np.array([float(y) for y, _ in valid])
            vals = np.array([v for _, v in valid])
            slope, _, r_value, p_value, _ = linregress(yrs, vals)
            trend = {
                "slope_pct_per_year": round(float(slope), 3),
                "r_squared": round(float(r_value ** 2), 4),
                "p_value": round(float(p_value), 4),
                "direction": (
                    "improving" if slope > 0.1 and p_value < 0.10
                    else "worsening" if slope < -0.1 and p_value < 0.10
                    else "stable"
                ),
            }

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": country,
                "series_id": "EG.ELC.RNEW.ZS",
                "latest_year": latest_year,
                "renewables_pct": round(renewables_pct, 2),
                "readiness_threshold_pct": self.READINESS_THRESHOLD,
                "gap_to_threshold_pct": round(
                    max(0.0, self.READINESS_THRESHOLD - renewables_pct), 2
                ),
                "n_obs": len(valid),
                "trend": trend,
                "low_readiness": renewables_pct < self.READINESS_THRESHOLD,
            },
        }
