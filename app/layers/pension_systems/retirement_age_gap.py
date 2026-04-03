"""Retirement Age Gap module.

Measures adequacy of retirement age relative to life expectancy. As longevity
rises without corresponding retirement age adjustments, pension systems face
longer payout periods and growing fiscal stress.

Score = clip(max(0, life_exp - 72) * 3, 0, 100)

A life expectancy of 72+ indicates retirement systems designed for shorter
lifespans are under-adjusted. Each year above 72 adds 3 stress points.

Sources: WDI SP.DYN.LE00.IN (life expectancy at birth, total years)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class RetirementAgeGap(LayerBase):
    layer_id = "lPS"
    name = "Retirement Age Gap"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.DYN.LE00.IN'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no life expectancy data"}

        valid = [(r["date"], float(r["value"])) for r in rows if r["value"] is not None]
        if not valid:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid life expectancy data"}

        dates, values = zip(*valid)
        life_exp = float(values[-1])
        score = float(np.clip(max(0.0, life_exp - 72.0) * 3.0, 0, 100))

        trend_slope = None
        gain_10yr = None
        if len(values) >= 5:
            x = np.arange(len(values), dtype=float)
            slope, _, _, _, _ = linregress(x, np.array(values))
            trend_slope = round(float(slope), 4)
            if len(values) >= 10:
                gain_10yr = round(float(values[-1]) - float(values[-10]), 2)

        return {
            "score": round(score, 1),
            "country": country,
            "life_expectancy_years": round(life_exp, 2),
            "latest_date": dates[-1],
            "benchmark_retirement_age": 72,
            "gap_above_benchmark": round(max(0.0, life_exp - 72.0), 2),
            "n_obs": len(values),
            "period": f"{dates[0]} to {dates[-1]}",
            "trend_slope_per_year": trend_slope,
            "life_exp_gain_10yr": gain_10yr,
            "interpretation": (
                "critical longevity-retirement mismatch" if score > 75
                else "significant longevity pressure" if score > 50
                else "moderate longevity pressure" if score > 25
                else "retirement age broadly adequate"
            ),
            "sources": ["WDI SP.DYN.LE00.IN"],
        }
