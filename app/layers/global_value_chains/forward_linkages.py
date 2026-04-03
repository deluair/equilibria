"""Forward Linkages module.

Measures export sophistication trend via manufacturing export share growth.

Countries with strong forward GVC linkages supply intermediate goods to
downstream manufacturers. A rising share of manufactured exports signals
movement up the value chain and stronger forward linkage positions.

Uses linear regression (scipy linregress) on TX.VAL.MANF.ZS.UN over time.
Declining slope = weakening forward linkages = GVC stress.

Score = clip(50 - slope * 100, 0, 100).
  slope = 0     -> score 50 (neutral)
  slope > 0.5   -> score ~0 (improving forward linkages, low stress)
  slope < -0.5  -> score ~100 (severe deterioration)

Sources: World Bank WDI (TX.VAL.MANF.ZS.UN).
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class ForwardLinkages(LayerBase):
    layer_id = "lVC"
    name = "Forward Linkages"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'TX.VAL.MANF.ZS.UN'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient manufacturing export data"}

        vals = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]
        x = np.arange(len(vals), dtype=float)

        slope, intercept, r_value, p_value, std_err = linregress(x, vals)

        score = float(np.clip(50.0 - slope * 100.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "manf_export_trend_slope": round(float(slope), 5),
            "r_squared": round(float(r_value ** 2), 4),
            "p_value": round(float(p_value), 4),
            "mean_manf_exports_pct": round(float(np.mean(vals)), 2),
            "latest_manf_exports_pct": round(float(vals[-1]), 2),
            "period": f"{dates[0]} to {dates[-1]}",
            "n_obs": len(vals),
            "interpretation": (
                "improving forward linkages" if slope > 0.1
                else "stable forward linkages" if slope >= -0.1
                else "declining forward linkages"
            ),
        }
