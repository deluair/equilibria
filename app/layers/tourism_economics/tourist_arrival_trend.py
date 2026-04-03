"""Tourist Arrival Trend module.

Tracks the trend in international tourist arrivals (ST.INT.ARVL — World Bank WDI)
using OLS regression over available observations. A declining trend raises the
score (stress); sustained growth lowers it.

Score: 0 (strong growth) to 100 (severe decline / no arrivals).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TouristArrivalTrend(LayerBase):
    layer_id = "lTO"
    name = "Tourist Arrival Trend"

    async def compute(self, db, **kwargs) -> dict:
        code = "ST.INT.ARVL"
        name = "international arrivals"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for ST.INT.ARVL (international tourist arrivals)",
            }

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if len(values) < 3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient observations for arrival trend (need >= 3)",
            }

        # Rows are DESC; reverse for chronological order
        arr = np.array(values[::-1])
        t = np.arange(len(arr), dtype=float)

        # OLS slope (normalised by mean to get relative growth rate)
        mean_val = float(np.mean(arr))
        if mean_val == 0:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "mean arrivals is zero, cannot compute trend",
            }

        slope, intercept = np.polyfit(t, arr, 1)
        relative_slope = slope / mean_val  # growth rate per period

        latest = float(arr[-1])
        pct_change_recent = (arr[-1] - arr[0]) / (abs(arr[0]) + 1e-10) * 100

        # Negative relative slope = declining trend = higher stress score
        # Map: relative_slope > 0.05 -> 20, ~0 -> 50, < -0.10 -> 85
        trend_score = float(np.clip(50 - relative_slope * 400, 10, 95))

        return {
            "score": round(trend_score, 1),
            "indicator": code,
            "latest_arrivals": round(latest, 0),
            "relative_slope_per_period": round(relative_slope, 4),
            "total_pct_change": round(pct_change_recent, 1),
            "n_obs": len(values),
            "trend_direction": "growing" if relative_slope > 0.01 else (
                "declining" if relative_slope < -0.01 else "stable"
            ),
            "methodology": "score = clip(50 - rel_slope * 400, 10, 95); OLS on arrivals series",
        }
