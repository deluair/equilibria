"""Old-age dependency stress: pension and fiscal burden from aging population.

The old-age dependency ratio (elderly per 100 working-age persons) is the
primary actuarial pressure on pay-as-you-go pension systems, public health
budgets, and long-term care financing. As the ratio rises, fewer workers must
support each retiree, compressing fiscal space and threatening intergenerational
equity. The Aaron (1966) condition for PAYGO sustainability requires that wage
bill growth exceeds the implicit return on alternative investments.

Indicator: SP.POP.DPND.OL (World Bank WDI)
    Elderly (65+) per 100 working-age (15-64)
Low stress: ratio < 10 (young demographics)
Moderate stress: 10-20
High stress: 20-35
Crisis: > 35

Score (0-100): higher = greater fiscal and pension stress

References:
    Aaron, H. (1966). The social insurance paradox. Canadian Journal of
        Economics and Political Science, 32(3), 371-374.
    Gruber, J. & Wise, D. (eds.) (1999). Social Security and Retirement
        around the World. University of Chicago Press.
    United Nations (2019). World Population Ageing 2019. DESA/POP/2019/ST.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class OldAgeDependencyStress(LayerBase):
    layer_id = "lDT"
    name = "Old-Age Dependency Stress"

    async def compute(self, db, **kwargs) -> dict:
        code = "SP.POP.DPND.OL"
        name = "age dependency ratio old"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no old-age dependency data"}

        values = [row["value"] for row in rows if row["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no old-age dependency data"}

        latest = float(values[0])
        avg = float(np.mean(values))
        trend = float(values[-1] - values[0]) if len(values) > 1 else 0.0

        score = _dependency_to_score(latest)
        score = float(np.clip(score, 0, 100))

        # Simple linear 10-year projection from trend
        obs_span = min(len(values), 15)
        annual_change = trend / obs_span if obs_span > 0 else 0.0
        projected_10y = round(max(0, latest + annual_change * 10), 2)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "old_age_dependency_ratio": round(latest, 2),
            "avg_15y": round(avg, 2),
            "annual_change": round(annual_change, 3),
            "projected_ratio_10y": projected_10y,
            "aging_accelerating": annual_change > 0,
            "stress_tier": _stress_tier(latest),
            "n_obs": len(values),
            "indicator": code,
        }


def _dependency_to_score(ratio: float) -> float:
    if ratio < 10.0:
        return 10.0
    if ratio < 15.0:
        return 10.0 + (ratio - 10.0) * 2.0
    if ratio < 25.0:
        return 20.0 + (ratio - 15.0) * 2.0
    if ratio < 35.0:
        return 40.0 + (ratio - 25.0) * 2.5
    if ratio < 45.0:
        return 65.0 + (ratio - 35.0) * 2.5
    return min(100.0, 90.0 + (ratio - 45.0) * 1.0)


def _stress_tier(ratio: float) -> str:
    if ratio < 10.0:
        return "low"
    if ratio < 20.0:
        return "moderate"
    if ratio < 35.0:
        return "high"
    return "crisis"
