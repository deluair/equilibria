"""Value Capture module.

Measures domestic value added in exports via high-technology export share.

1. **High-tech exports** (TX.VAL.TECH.MF.ZS): high-technology exports as % of
   manufactured exports. Higher share indicates the country retains more value
   in complex, knowledge-intensive products rather than low-margin assembly.

2. **Score**: low high-tech share = low value capture = more GVC stress.
   Score = clip(max(0, 20 - hitech_pct) * 3.33, 0, 100).
   At hitech_pct=0 -> score=66.6; at hitech_pct=20 -> score=0 (captured).

Sources: World Bank WDI (TX.VAL.TECH.MF.ZS).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ValueCapture(LayerBase):
    layer_id = "lVC"
    name = "Value Capture"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'TX.VAL.TECH.MF.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no high-tech export data"}

        vals = np.array([float(r["value"]) for r in rows])
        hitech_pct = float(np.mean(vals))
        latest = float(vals[0])

        # Trend
        trend_slope = None
        if len(vals) >= 4:
            x = np.arange(len(vals), dtype=float)
            trend_slope = float(np.polyfit(x, vals[::-1], 1)[0])  # ascending time order

        score = float(np.clip(max(0.0, 20.0 - hitech_pct) * 3.33, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "hitech_exports_pct": round(hitech_pct, 2),
            "latest_hitech_pct": round(latest, 2),
            "trend_slope_pct_per_yr": round(trend_slope, 4) if trend_slope is not None else None,
            "n_obs": len(vals),
            "interpretation": (
                "low value capture (commodity/assembly-led)" if hitech_pct < 10
                else "moderate value capture" if hitech_pct < 25
                else "high value capture (knowledge-intensive)"
            ),
        }
