"""Labor productivity growth trend analysis.

Output per worker growth is the foundation of sustainable wage growth and
living standards. Sustained below-1%/yr growth signals structural stagnation.

Proxy: GDP per capita growth (NY.GDP.PCAP.KD.ZG) from WDI. In the absence of
employment-normalized output series, GDP per capita growth closely tracks
labor productivity growth for most developing economies.

Method: OLS linear regression of annual growth rates over the available window
(up to 15 years). The slope of the trend (acceleration/deceleration) and the
mean growth rate both feed into the stress score.

Scoring:
    - mean growth < 0%/yr        -> score = 80 + |mean| * 5  (capped at 100)
    - mean growth in [0, 1)      -> score = 60 - mean * 10
    - mean growth in [1, 3)      -> score = 30 - (mean - 1) * 10
    - mean growth >= 3%          -> score = max(5, 10 - (mean - 3) * 2)
    Negative slope (decelerating) amplifies score by up to 20%.

Sources: WDI (NY.GDP.PCAP.KD.ZG)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase

SERIES = "NY.GDP.PCAP.KD.ZG"


class LaborProductivityGrowth(LayerBase):
    layer_id = "l3"
    name = "Labor Productivity Growth"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD.ZG'
              AND dp.value IS NOT NULL
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient GDP per capita growth data"}

        dates = [r["date"] for r in rows]
        vals = np.array([float(r["value"]) for r in rows], dtype=float)

        # Limit to last 15 observations
        if len(vals) > 15:
            dates = dates[-15:]
            vals = vals[-15:]

        n = len(vals)
        t_idx = np.arange(n, dtype=float)
        slope, intercept, r_value, p_value, se = linregress(t_idx, vals)
        mean_growth = float(np.mean(vals))
        r_squared = float(r_value ** 2)

        # Score based on mean growth level
        if mean_growth < 0:
            score = min(100.0, 80.0 + abs(mean_growth) * 5.0)
        elif mean_growth < 1.0:
            score = 60.0 - mean_growth * 10.0
        elif mean_growth < 3.0:
            score = 30.0 - (mean_growth - 1.0) * 10.0
        else:
            score = max(5.0, 10.0 - (mean_growth - 3.0) * 2.0)

        # Amplify if trend is decelerating (negative slope)
        if slope < 0 and p_value < 0.20:
            score = min(100.0, score * (1.0 + min(0.20, abs(slope) * 0.05)))

        score = float(np.clip(score, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "n_periods": n,
            "mean_growth_pct": round(mean_growth, 3),
            "trend": {
                "slope_pct_per_yr": round(float(slope), 4),
                "r_squared": round(r_squared, 4),
                "p_value": round(float(p_value), 4),
                "direction": "accelerating" if slope > 0.05 else "decelerating" if slope < -0.05 else "stable",
            },
            "recent_growth_pct": round(float(vals[-1]), 3),
            "time_range": {"start": dates[0], "end": dates[-1]},
            "proxy": "NY.GDP.PCAP.KD.ZG (GDP per capita growth, constant USD)",
            "note": "Sustained below 1%/yr = productivity stagnation stress",
        }
