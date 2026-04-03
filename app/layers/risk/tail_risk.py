"""Tail Risk module.

GDP growth tail risk: probability of severe economic contraction.
Queries WDI: NY.GDP.MKTP.KD.ZG (GDP growth, annual %).

Methodology:
  - Fit empirical distribution of historical GDP growth rates.
  - Compute left tail at 5th percentile.
  - More negative tail value -> higher tail risk score.

Score = clip(-tail_5pct * 8, 0, 100)
  Example: tail_5pct = -5% -> score = 40
           tail_5pct = -10% -> score = 80

Sources: World Bank WDI.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class TailRisk(LayerBase):
    layer_id = "lRI"
    name = "Tail Risk"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.MKTP.KD.ZG'
              AND dp.value IS NOT NULL
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 10:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient GDP growth data (need >= 10 obs)",
            }

        growth = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]

        tail_5pct = float(np.percentile(growth, 5))
        tail_1pct = float(np.percentile(growth, 1))

        # Kernel density estimate for smoother tail
        kde = stats.gaussian_kde(growth)
        grid = np.linspace(growth.min() - 1, 0, 200)
        left_prob = float(kde.integrate_box_1d(-np.inf, -3.0))

        score = float(np.clip(-tail_5pct * 8, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(growth),
            "period": f"{dates[0]} to {dates[-1]}",
            "distribution": {
                "mean": round(float(np.mean(growth)), 4),
                "std": round(float(np.std(growth)), 4),
                "skewness": round(float(stats.skew(growth)), 4),
                "kurtosis": round(float(stats.kurtosis(growth)), 4),
                "min": round(float(growth.min()), 4),
                "max": round(float(growth.max()), 4),
            },
            "tail_estimates": {
                "p5_pct": round(tail_5pct, 4),
                "p1_pct": round(tail_1pct, 4),
                "prob_below_neg3pct": round(left_prob, 4),
            },
        }
