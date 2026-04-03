"""Aging workforce pressure: elderly share trend and labor supply implications.

A rising share of population aged 65 and over progressively tightens labor
supply, increases the demand for care-intensive services, reshapes consumption
patterns toward healthcare and leisure, and pressures public pension and health
systems. Acemoglu and Restrepo (2017) find that aging economies adopt automation
faster; Cutler et al. (1990) document the fiscal implications of demographic
aging for government budgets. The speed of increase matters as much as the
level: rapid aging (> 0.3 pp/year) gives institutions less time to adapt.

Indicator: SP.POP.65UP.TO.ZS (World Bank WDI)
    Population ages 65 and above (% of total)
Low pressure: < 7% (young economy)
Moderate: 7-14%
High: 14-21%
Super-aged (UN classification): > 21%

Score (0-100): higher = greater aging workforce pressure

References:
    Acemoglu, D. & Restrepo, P. (2017). Secular stagnation? The effect of
        aging on economic growth in the age of automation. American Economic
        Review, 107(5), 174-179.
    Cutler, D., Poterba, J., Sheiner, L. & Summers, L. (1990). An aging
        society: opportunity or challenge? Brookings Papers on Economic
        Activity, 1990(1), 1-73.
    United Nations (2019). World Population Ageing 2019. DESA/POP/2019/ST.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class AgingWorkforcePressure(LayerBase):
    layer_id = "lDT"
    name = "Aging Workforce Pressure"

    async def compute(self, db, **kwargs) -> dict:
        code = "SP.POP.65UP.TO.ZS"
        name = "population ages 65 and above"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no elderly population share data"}

        values = [row["value"] for row in rows if row["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no elderly population share data"}

        latest = float(values[0])
        avg = float(np.mean(values))
        trend = float(values[-1] - values[0]) if len(values) > 1 else 0.0
        obs_span = min(len(values), 15)
        annual_increase = trend / obs_span if obs_span > 0 else 0.0

        super_aged = latest > 21.0
        rapid_aging = annual_increase > 0.3

        score = _elderly_share_to_score(latest)
        if rapid_aging:
            score = min(100.0, score + 8.0)
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "elderly_share_pct": round(latest, 2),
            "avg_elderly_share_15y": round(avg, 2),
            "annual_increase_pp": round(annual_increase, 3),
            "super_aged": super_aged,
            "rapid_aging": rapid_aging,
            "aging_stage": _aging_stage(latest),
            "n_obs": len(values),
            "indicator": code,
        }


def _elderly_share_to_score(share: float) -> float:
    if share < 7.0:
        return 10.0
    if share < 14.0:
        return 10.0 + (share - 7.0) * 3.0
    if share < 21.0:
        return 31.0 + (share - 14.0) * 5.0
    return min(100.0, 66.0 + (share - 21.0) * 3.5)


def _aging_stage(share: float) -> str:
    if share < 7.0:
        return "young"
    if share < 14.0:
        return "aging"
    if share < 21.0:
        return "aged"
    return "super-aged"
