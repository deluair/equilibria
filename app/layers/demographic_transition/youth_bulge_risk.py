"""Youth bulge risk: political and economic instability from high youth share.

A youth bulge (population under 15 exceeding ~35% of total) correlates with
elevated risks of political instability, unemployment, and social conflict when
labor markets cannot absorb the large cohort entering working age. Urdal (2006)
finds that youth bulges significantly increase the risk of armed conflict; Bloom
and Williamson (1998) link youth dependency to slower economic growth. However,
a moderate youth share signals future demographic dividend if institutions invest
in health and education.

Indicator: SP.POP.0014.TO.ZS (World Bank WDI)
    Population ages 0-14 as % of total
Low risk: < 20% (post-transition, aging society)
Moderate: 20-30%
High risk (bulge): 30-40%
Extreme (instability risk): > 40%

Score (0-100): higher = greater youth bulge instability risk

References:
    Urdal, H. (2006). A clash of generations? Youth bulges and political
        violence. International Studies Quarterly, 50(3), 607-629.
    Bloom, D. & Williamson, J. (1998). Demographic transitions and economic
        miracles in emerging Asia. World Bank Economic Review, 12(3), 419-455.
    Population Action International (2006). The Shape of Things to Come:
        Why Age Structure Matters to a Safer, More Equitable World.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

BULGE_THRESHOLD = 35.0
HIGH_RISK_THRESHOLD = 40.0


class YouthBulgeRisk(LayerBase):
    layer_id = "lDT"
    name = "Youth Bulge Risk"

    async def compute(self, db, **kwargs) -> dict:
        code = "SP.POP.0014.TO.ZS"
        name = "population ages 0-14"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no youth population data"}

        values = [row["value"] for row in rows if row["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no youth population data"}

        latest = float(values[0])
        avg = float(np.mean(values))
        trend = float(values[-1] - values[0]) if len(values) > 1 else 0.0

        is_youth_bulge = latest >= BULGE_THRESHOLD
        extreme_risk = latest >= HIGH_RISK_THRESHOLD

        score = _youth_share_to_score(latest)
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "youth_share_pct": round(latest, 2),
            "is_youth_bulge": is_youth_bulge,
            "extreme_instability_risk": extreme_risk,
            "bulge_threshold_pct": BULGE_THRESHOLD,
            "avg_15y": round(avg, 2),
            "trend_direction": "declining" if trend < -1.0 else "rising" if trend > 1.0 else "stable",
            "n_obs": len(values),
            "indicator": code,
        }


def _youth_share_to_score(share: float) -> float:
    if share < 20.0:
        return 20.0 - share * 0.5
    if share < 30.0:
        return 20.0 + (share - 20.0) * 1.5
    if share < 40.0:
        return 35.0 + (share - 30.0) * 3.5
    return min(100.0, 70.0 + (share - 40.0) * 3.0)
