"""Life expectancy gains: trend analysis and convergence to frontier.

Rising life expectancy reflects improvements in nutrition, healthcare access,
sanitation, and income. Sustained gains indicate demographic transition
progression and accumulation of health human capital. Stagnating or declining
life expectancy (a Marmot (2010) warning indicator) signals crisis in public
health infrastructure or rising inequality. The gap from the global frontier
(currently ~85 years in Japan/Switzerland) measures accumulated human capital
deficit.

Indicator: SP.DYN.LE00.IN (World Bank WDI)
    Life expectancy at birth (total years)
Frontier (high-income average): ~82 years
Low stress threshold: > 75 years
Moderate concern: 65-75 years
High concern: < 65 years

Score (0-100): higher = greater health/human-capital deficit
    LE > 80: score ~ 10 (STABLE)
    LE 70-80: score 10-40
    LE 60-70: score 40-70
    LE < 60: score 70-100

References:
    Marmot, M. (2010). Fair Society, Healthy Lives. The Marmot Review.
    Oeppen, J. & Vaupel, J.W. (2002). Broken limits to life expectancy.
        Science, 296(5570), 1029-1031.
    WHO (2023). World Health Statistics 2023. World Health Organization.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

FRONTIER_LE = 82.0
GOOD_THRESHOLD = 75.0
MODERATE_THRESHOLD = 65.0


class LifeExpectancyGains(LayerBase):
    layer_id = "lDT"
    name = "Life Expectancy Gains"

    async def compute(self, db, **kwargs) -> dict:
        code = "SP.DYN.LE00.IN"
        name = "life expectancy at birth"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no life expectancy data"}

        values = [row["value"] for row in rows if row["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no life expectancy data"}

        latest = float(values[0])
        avg = float(np.mean(values))
        trend = float(values[-1] - values[0]) if len(values) > 1 else 0.0
        obs_span = min(len(values), 15)
        annual_gain = trend / obs_span if obs_span > 0 else 0.0

        gap_from_frontier = round(FRONTIER_LE - latest, 2)
        stagnating = abs(annual_gain) < 0.05

        score = _le_to_score(latest)
        # Stagnation or decline penalty
        if annual_gain < -0.1:
            score = min(100.0, score + 15.0)
        elif stagnating:
            score = min(100.0, score + 5.0)
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "life_expectancy_years": round(latest, 1),
            "frontier_le_years": FRONTIER_LE,
            "gap_from_frontier_years": gap_from_frontier,
            "annual_gain_years": round(annual_gain, 3),
            "trend_direction": "improving" if annual_gain > 0.05 else "declining" if annual_gain < -0.05 else "stagnant",
            "avg_le_15y": round(avg, 1),
            "n_obs": len(values),
            "indicator": code,
        }


def _le_to_score(le: float) -> float:
    if le >= 80.0:
        return 10.0
    if le >= 75.0:
        return 10.0 + (80.0 - le) * 3.0
    if le >= 65.0:
        return 25.0 + (75.0 - le) * 3.5
    if le >= 55.0:
        return 60.0 + (65.0 - le) * 3.0
    return min(100.0, 90.0 + (55.0 - le) * 1.0)
