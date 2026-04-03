"""Urbanization transition: urban growth speed and infrastructure stress.

Urban growth rate measures the speed of population shift from rural to urban
areas. Moderate urbanization supports agglomeration economies, productivity
gains, and structural transformation (Lewis 1954, Henderson 2003). Rapid
urbanization (> 4% annually) outpaces infrastructure and housing capacity,
generating slums, congestion, and environmental degradation. Negative urban
growth signals de-urbanization or urban crisis. The transition from rural to
majority-urban society is a defining feature of economic development; the pace
determines whether gains or costs dominate.

Indicator: SP.URB.GROW (World Bank WDI)
    Urban population growth rate (annual %)
Optimal: 1.5-3.5% (fast but manageable)
Stress: > 5% or < 0%

Score (0-100): higher = greater urbanization stress

References:
    Lewis, W.A. (1954). Economic development with unlimited supplies of
        labour. Manchester School, 22(2), 139-191.
    Henderson, J.V. (2003). The urbanization process and economic growth:
        the so-what question. Journal of Economic Growth, 8(1), 47-71.
    UN-Habitat (2022). World Cities Report 2022. United Nations.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

OPTIMAL_LOW = 1.5
OPTIMAL_HIGH = 3.5


class UrbanizationTransition(LayerBase):
    layer_id = "lDT"
    name = "Urbanization Transition"

    async def compute(self, db, **kwargs) -> dict:
        code = "SP.URB.GROW"
        name = "urban population growth"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no urban growth data"}

        values = [row["value"] for row in rows if row["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no urban growth data"}

        latest = float(values[0])
        avg = float(np.mean(values))
        volatility = float(np.std(values)) if len(values) > 1 else 0.0

        in_optimal_band = OPTIMAL_LOW <= latest <= OPTIMAL_HIGH
        rapid_urbanization = latest > 5.0
        de_urbanizing = latest < 0.0

        score = _urban_growth_to_score(latest)
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "urban_growth_rate_pct": round(latest, 3),
            "optimal_band": [OPTIMAL_LOW, OPTIMAL_HIGH],
            "in_optimal_band": in_optimal_band,
            "rapid_urbanization": rapid_urbanization,
            "de_urbanizing": de_urbanizing,
            "avg_urban_growth_15y": round(avg, 3),
            "growth_volatility": round(volatility, 3),
            "n_obs": len(values),
            "indicator": code,
        }


def _urban_growth_to_score(rate: float) -> float:
    if OPTIMAL_LOW <= rate <= OPTIMAL_HIGH:
        mid = (OPTIMAL_LOW + OPTIMAL_HIGH) / 2.0
        return 10.0 + abs(rate - mid) * 5.0
    if rate < OPTIMAL_LOW:
        if rate >= 0.0:
            return 15.0 + (OPTIMAL_LOW - rate) * 12.0
        return min(100.0, 33.0 + abs(rate) * 30.0)
    if rate <= 5.0:
        return 15.0 + (rate - OPTIMAL_HIGH) * 12.0
    return min(100.0, 39.0 + (rate - 5.0) * 20.0)
