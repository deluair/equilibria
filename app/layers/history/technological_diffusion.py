"""Technological Diffusion module.

Measures internet adoption lag relative to the 70% high-income benchmark.
Countries far below 70% penetration have failed to absorb modern
information-communication technology, constraining productivity growth.

Indicator: IT.NET.USER.ZS (Individuals using the Internet, % of population, WDI).
Benchmark: 70% (approximate high-income country average).
Score: max(0, 70 - latest_value) * 1.4286
  - 70% or above -> 0   (no lag)
  - 0%           -> 100 (completely behind)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_BENCHMARK_PCT = 70.0
_SCALE = 100.0 / _BENCHMARK_PCT  # 1.4286


class TechnologicalDiffusion(LayerBase):
    layer_id = "lHI"
    name = "Technological Diffusion"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IT.NET.USER.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        latest_value = float(rows[0]["value"])
        lag = max(0.0, _BENCHMARK_PCT - latest_value)
        score = float(np.clip(lag * _SCALE, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "internet_users_pct": round(latest_value, 2),
            "benchmark_pct": _BENCHMARK_PCT,
            "lag_pct_points": round(lag, 2),
            "latest_year": rows[0]["date"][:4],
        }
