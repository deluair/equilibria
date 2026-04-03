"""Population growth stress: excessive or negative growth as systemic risk.

Both extremes of population growth impose economic stress. Excessive growth
(> 3% annually) strains food systems, urban infrastructure, and public services,
compressing per-capita investment. Negative growth creates labor shortages,
shrinking domestic markets, and rising pension burdens. The optimal band (roughly
0.5-1.5%) supports steady expansion of the productive base without overwhelming
public capacity. Kremer (1993) and Boserup (1965) debate whether population
pressure spurs or retards innovation; the empirical consensus favors a middle path.

Indicator: SP.POP.GROW (World Bank WDI)
    Population growth (annual %)
Optimal: 0.5-1.5%
Moderate concern: < 0.0% or > 2.5%
High concern: < -0.5% or > 3.5%

Score (0-100): higher = greater growth-related stress

References:
    Kremer, M. (1993). Population growth and technological change: One
        million B.C. to 1990. Quarterly Journal of Economics, 108(3), 681-716.
    Boserup, E. (1965). The Conditions of Agricultural Growth. Aldine.
    Malthus, T.R. (1798). An Essay on the Principle of Population. J. Johnson.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

OPTIMAL_LOW = 0.5
OPTIMAL_HIGH = 1.5


class PopulationGrowthStress(LayerBase):
    layer_id = "lDT"
    name = "Population Growth Stress"

    async def compute(self, db, **kwargs) -> dict:
        code = "SP.POP.GROW"
        name = "population growth annual"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no population growth data"}

        values = [row["value"] for row in rows if row["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no population growth data"}

        latest = float(values[0])
        avg = float(np.mean(values))
        volatility = float(np.std(values)) if len(values) > 1 else 0.0

        in_optimal_band = OPTIMAL_LOW <= latest <= OPTIMAL_HIGH
        negative_growth = latest < 0.0
        excessive_growth = latest > 3.0

        score = _growth_to_score(latest)
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "population_growth_pct": round(latest, 3),
            "optimal_band": [OPTIMAL_LOW, OPTIMAL_HIGH],
            "in_optimal_band": in_optimal_band,
            "negative_growth": negative_growth,
            "excessive_growth": excessive_growth,
            "avg_growth_15y": round(avg, 3),
            "growth_volatility": round(volatility, 3),
            "n_obs": len(values),
            "indicator": code,
        }


def _growth_to_score(rate: float) -> float:
    if OPTIMAL_LOW <= rate <= OPTIMAL_HIGH:
        mid = (OPTIMAL_LOW + OPTIMAL_HIGH) / 2.0
        return 10.0 + abs(rate - mid) * 5.0
    if rate < OPTIMAL_LOW:
        if rate >= 0.0:
            return 15.0 + (OPTIMAL_LOW - rate) * 15.0
        if rate >= -0.5:
            return 22.5 + abs(rate) * 25.0
        return min(100.0, 35.0 + abs(rate + 0.5) * 50.0)
    if rate <= 2.5:
        return 15.0 + (rate - OPTIMAL_HIGH) * 15.0
    if rate <= 3.5:
        return 22.5 + (rate - 2.5) * 30.0
    return min(100.0, 52.5 + (rate - 3.5) * 25.0)
