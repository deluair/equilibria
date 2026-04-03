"""Electrification gap: population without electricity access.

Queries World Bank WDI series EG.ELC.ACCS.ZS (access to electricity as %
of population). Lack of electricity access is a fundamental energy poverty
indicator. The gap from universal access (100%) directly maps to stress.

Score = max(0, 100 - access_pct):
  - 100% access -> score 0  (no gap, no stress)
  - 80% access  -> score 20
  - 50% access  -> score 50
  - 0% access   -> score 100 (maximum stress)

Sources: World Bank WDI (EG.ELC.ACCS.ZS)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class ElectrificationGap(LayerBase):
    layer_id = "l16"
    name = "Electrification Gap"
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
            WHERE ds.series_id = 'EG.ELC.ACCS.ZS'
              AND ds.country_iso3 = ?
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no electricity access data",
            }

        valid = [(r["date"][:4], float(r["value"])) for r in rows if r["value"] is not None]

        if not valid:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "all electricity access values are null",
            }

        latest_year, access_pct = valid[-1]

        score = float(np.clip(max(0.0, 100.0 - access_pct), 0, 100))

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

        gap_pct = max(0.0, 100.0 - access_pct)
        severity = (
            "critical" if gap_pct >= 50
            else "severe" if gap_pct >= 20
            else "moderate" if gap_pct >= 5
            else "minimal"
        )

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": country,
                "series_id": "EG.ELC.ACCS.ZS",
                "latest_year": latest_year,
                "access_pct": round(access_pct, 2),
                "gap_pct": round(gap_pct, 2),
                "severity": severity,
                "n_obs": len(valid),
                "trend": trend,
                "universal_access": access_pct >= 99.5,
            },
        }
