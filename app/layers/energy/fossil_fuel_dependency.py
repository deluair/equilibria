"""Fossil fuel dependency: fossil share of energy consumption as transition risk.

Queries World Bank WDI series EG.USE.COMM.FO.ZS (fossil fuel energy
consumption as % of total). High fossil share signals vulnerability to
carbon pricing, stranded-asset risk, and energy transition disruption.

Score = clip(fossil_pct * 0.9, 0, 100):
  - 100% fossil -> score 90 (near-maximum stress)
  - 50% fossil  -> score 45
  - 0% fossil   -> score 0

Sources: World Bank WDI (EG.USE.COMM.FO.ZS)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class FossilFuelDependency(LayerBase):
    layer_id = "l16"
    name = "Fossil Fuel Dependency"
    weight = 0.20

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
            WHERE ds.series_id = 'EG.USE.COMM.FO.ZS'
              AND ds.country_iso3 = ?
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no fossil fuel consumption data",
            }

        valid = [(r["date"][:4], float(r["value"])) for r in rows if r["value"] is not None]

        if not valid:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "all fossil fuel values are null",
            }

        latest_year, fossil_pct = valid[-1]

        score = float(np.clip(fossil_pct * 0.9, 0, 100))

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
                    "declining" if slope < -0.1 and p_value < 0.10
                    else "rising" if slope > 0.1 and p_value < 0.10
                    else "stable"
                ),
            }

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": country,
                "series_id": "EG.USE.COMM.FO.ZS",
                "latest_year": latest_year,
                "fossil_pct": round(fossil_pct, 2),
                "n_obs": len(valid),
                "trend": trend,
                "high_dependency": fossil_pct >= 80.0,
            },
        }
