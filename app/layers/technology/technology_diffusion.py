"""Technology Diffusion module.

Technology adoption convergence: internet penetration growth rate
vs high-income benchmark (80% as reference).

Lagging diffusion = larger technology gap = higher stress score.

Source: WDI (IT.NET.USER.ZS)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase

# High-income benchmark for internet penetration (%)
_BENCHMARK_INTERNET_PCT = 80.0


class TechnologyDiffusion(LayerBase):
    layer_id = "lTE"
    name = "Technology Diffusion"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'IT.NET.USER.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient internet penetration data",
            }

        vals = np.array([float(r["value"]) for r in rows])
        latest = float(vals[-1])

        # Technology gap relative to benchmark
        gap_pct = max(0.0, _BENCHMARK_INTERNET_PCT - latest)

        # Growth rate trend (recent 5 years or available)
        window = min(5, len(vals))
        recent = vals[-window:]
        t = np.arange(len(recent), dtype=float)
        slope = 0.0
        if len(recent) >= 3:
            slope, _, _, _, _ = linregress(t, recent)

        # Slow growth + large gap = high stress
        gap_score = float(np.clip(gap_pct * 1.0, 0.0, 70.0))
        # Slow diffusion penalty: slope < 1 pp/year gets penalized
        growth_penalty = float(np.clip(max(0.0, 1.0 - slope) * 30.0, 0.0, 30.0))

        score = float(np.clip(gap_score + growth_penalty, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "internet_pct_latest": round(latest, 2),
            "benchmark_pct": _BENCHMARK_INTERNET_PCT,
            "technology_gap_pct": round(gap_pct, 2),
            "recent_growth_slope_pp_yr": round(float(slope), 4),
            "n_obs": len(rows),
            "period": f"{rows[0]['date']} to {rows[-1]['date']}",
            "interpretation": "gap to benchmark + slow diffusion rate = technology adoption stress",
        }
