"""Inattention Economics module.

Sticky information proxy: inflation persistence via AR(1) coefficient.
AR(1) coefficient > 0.8 in inflation series implies high inertia -- agents
are not updating expectations frequently (sticky information / rational inattention).

Score = clip(ar1_coeff * 100, 0, 100)

Source: WDI FP.CPI.TOTL.ZG (Inflation, consumer prices, annual %)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


def _ar1_coeff(series: np.ndarray) -> float:
    """OLS estimate of AR(1) coefficient."""
    y = series[1:]
    x = series[:-1]
    if len(x) < 3 or np.std(x) < 1e-12:
        return 0.0
    # OLS: beta = cov(x, y) / var(x)
    cov = float(np.cov(x, y)[0, 1])
    var = float(np.var(x))
    return cov / var if var > 1e-12 else 0.0


class InattentionEconomics(LayerBase):
    layer_id = "l13"
    name = "Inattention Economics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FP.CPI.TOTL.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 8:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]

        ar1 = _ar1_coeff(values)
        # Clip to [0, 1] for scoring (negative AR1 = mean-reverting, not inattention)
        ar1_clipped = float(np.clip(ar1, 0, 1))
        score = float(np.clip(ar1_clipped * 100, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(values),
            "period": f"{dates[0]} to {dates[-1]}",
            "ar1_coefficient": round(ar1, 4),
            "mean_inflation": round(float(np.mean(values)), 2),
            "std_inflation": round(float(np.std(values)), 2),
            "high_persistence": ar1 > 0.8,
            "interpretation": "AR(1) > 0.8 indicates sticky information / rational inattention in inflation expectations",
        }
