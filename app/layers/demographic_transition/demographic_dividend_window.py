"""Demographic dividend window: working-age population share.

The demographic dividend arises when the working-age cohort (15-64) forms an
unusually large share of the total population, reducing the aggregate dependency
burden and creating a one-time growth opportunity if accompanied by the right
institutions and policies. Bloom, Canning and Sevilla (2003) estimate that the
demographic dividend accounted for roughly a third of East Asian growth between
1965 and 1990.

Indicator: SP.POP.1564.TO.ZS (World Bank WDI)
Optimal window: working-age share 65-70%
Below 55% or above 72%: signals imbalance

Score (0-100): higher score = greater demographic stress / missed dividend

References:
    Bloom, D., Canning, D. & Sevilla, J. (2003). The Demographic Dividend.
        RAND Corporation.
    Mason, A. (2005). Demographic transition and demographic dividends in
        developed and developing countries. UN Expert Group Meeting,
        UN/POP/PD/2005/2.
    Lee, R. & Mason, A. (2006). What is the demographic dividend?
        Finance and Development, 43(3).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

OPTIMAL_LOW = 65.0
OPTIMAL_HIGH = 70.0
DIVIDEND_PEAK = 67.5


class DemographicDividendWindow(LayerBase):
    layer_id = "lDT"
    name = "Demographic Dividend Window"

    async def compute(self, db, **kwargs) -> dict:
        code = "SP.POP.1564.TO.ZS"
        name = "population ages 15-64"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no working-age share data"}

        values = [row["value"] for row in rows if row["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no working-age share data"}

        latest = float(values[0])
        avg = float(np.mean(values))
        trend = float(values[-1] - values[0]) if len(values) > 1 else 0.0

        in_dividend_window = OPTIMAL_LOW <= latest <= OPTIMAL_HIGH
        distance_from_peak = abs(latest - DIVIDEND_PEAK)

        # Score: 0 at peak, rising as distance grows
        if in_dividend_window:
            score = distance_from_peak * 2.0
        elif latest < OPTIMAL_LOW:
            score = 10.0 + (OPTIMAL_LOW - latest) * 2.5
        else:
            score = 10.0 + (latest - OPTIMAL_HIGH) * 3.0

        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "working_age_share_pct": round(latest, 2),
            "optimal_range": [OPTIMAL_LOW, OPTIMAL_HIGH],
            "in_dividend_window": in_dividend_window,
            "distance_from_peak_pct": round(distance_from_peak, 2),
            "avg_15y": round(avg, 2),
            "trend_direction": "expanding" if trend > 0.2 else "contracting" if trend < -0.2 else "stable",
            "n_obs": len(values),
            "indicator": code,
        }
