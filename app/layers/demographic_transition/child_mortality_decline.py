"""Child mortality decline: under-5 mortality as demographic transition proxy.

Declining child mortality is both a driver and consequence of demographic
transition. High child mortality sustains high fertility as parents insure
against child loss (the insurance motive); as child survival improves, desired
family size falls. Preston (1980) demonstrated that income explains only half of
life expectancy gains; the remainder reflects public health infrastructure,
medical technology diffusion, and education. Under-5 mortality is therefore a
comprehensive indicator of health system quality, nutritional status, sanitation,
and caregiving capacity.

Indicator: SH.DYN.MORT (World Bank WDI)
    Mortality rate, under-5 (per 1,000 live births)
Low stress (good outcome): < 10 per 1,000
Moderate: 10-30 per 1,000
High: 30-60 per 1,000
Crisis: > 60 per 1,000

Score (0-100): higher = greater child mortality burden (worse outcome)

References:
    Preston, S.H. (1975). The changing relation between mortality and level
        of economic development. Population Studies, 29(2), 231-248.
    Black, R.E. et al. (2010). Global, regional, and national causes of
        child mortality in 2008. The Lancet, 375(9730), 1969-1987.
    UNICEF (2023). Levels and Trends in Child Mortality Report 2023.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ChildMortalityDecline(LayerBase):
    layer_id = "lDT"
    name = "Child Mortality Decline"

    async def compute(self, db, **kwargs) -> dict:
        code = "SH.DYN.MORT"
        name = "mortality rate under-5"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no child mortality data"}

        values = [row["value"] for row in rows if row["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no child mortality data"}

        latest = float(values[0])
        avg = float(np.mean(values))
        trend = float(values[-1] - values[0]) if len(values) > 1 else 0.0
        obs_span = min(len(values), 15)
        annual_change = trend / obs_span if obs_span > 0 else 0.0

        score = _mortality_to_score(latest)
        # Reward declining trend
        if annual_change < -1.0:
            score = max(0.0, score - 5.0)
        # Penalize rising mortality
        elif annual_change > 0.5:
            score = min(100.0, score + 10.0)
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "under5_mortality_per1000": round(latest, 2),
            "avg_15y": round(avg, 2),
            "annual_change_per1000": round(annual_change, 3),
            "trend_direction": "declining" if annual_change < -0.5 else "rising" if annual_change > 0.5 else "stable",
            "mortality_tier": _mortality_tier(latest),
            "n_obs": len(values),
            "indicator": code,
        }


def _mortality_to_score(rate: float) -> float:
    if rate < 10.0:
        return 5.0 + rate * 0.5
    if rate < 30.0:
        return 10.0 + (rate - 10.0) * 1.5
    if rate < 60.0:
        return 40.0 + (rate - 30.0) * 1.0
    if rate < 100.0:
        return 70.0 + (rate - 60.0) * 0.75
    return min(100.0, 100.0)


def _mortality_tier(rate: float) -> str:
    if rate < 10.0:
        return "low"
    if rate < 30.0:
        return "moderate"
    if rate < 60.0:
        return "high"
    return "crisis"
