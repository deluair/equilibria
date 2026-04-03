"""Life expectancy gap from a 75-year benchmark.

Life expectancy at birth is a summary measure of population health and
socioeconomic development (Preston 1975). Countries falling short of the
75-year benchmark face higher premature mortality, lower human capital
accumulation, compressed productive lifespans, and greater burden-of-disease
costs (WHO 2020).

The gap metric follows a linear penalty model: every year below 75 adds
roughly 3.3 score points, so a country at 45 years scores ~99 (near-CRISIS)
while a country at 75 or above scores 0 (STABLE). This is consistent with
the UN's use of the 75-year threshold in MPI and HDI benchmarking.

Score formula: max(0, 75 - life_expectancy) * 3.33, clipped to [0, 100].
At LE = 45: score = 99.9 -> 100 (CRISIS).
At LE = 60: score = 50 (STRESS boundary).
At LE = 75+: score = 0 (STABLE).

References:
    Preston, S. (1975). The Changing Relation between Mortality and Level of
        Economic Development. Population Studies, 29(2), 231-248.
    WHO (2020). World Health Statistics 2020. Geneva.
    UNDP (2023). Human Development Report 2023/24.

Series: SP.DYN.LE00.IN (life expectancy at birth, total years).
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class LifeExpectancy(LayerBase):
    layer_id = "l17"
    name = "Life Expectancy"
    weight = 0.20

    BENCHMARK_YEARS = 75.0
    SCORE_MULTIPLIER = 100.0 / BENCHMARK_YEARS  # ~1.333 per year below benchmark

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3")

        if not country_iso3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "country_iso3 required",
            }

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.DYN.LE00.IN'
              AND ds.country_iso3 = ?
              AND dp.value IS NOT NULL
            ORDER BY dp.date
            """,
            (country_iso3,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"no life expectancy data for {country_iso3}",
            }

        years = [int(r["date"][:4]) for r in rows]
        values = [float(r["value"]) for r in rows]

        latest_le = values[-1]
        latest_year = years[-1]

        # Trend via linear regression (need >= 5 obs)
        trend = None
        if len(values) >= 5:
            yr_arr = np.array(years)
            val_arr = np.array(values)
            slope, intercept, r, p, se = stats.linregress(yr_arr, val_arr)
            trend = {
                "annual_gain_yrs": round(float(slope), 4),
                "r_squared": round(float(r ** 2), 4),
                "p_value": round(float(p), 6),
                "n_obs": len(values),
                "improving": slope > 0,
            }

        # Gap from benchmark
        gap = max(0.0, self.BENCHMARK_YEARS - latest_le)

        # Score: each year below benchmark = 3.33 points (linear)
        # At LE=75 -> 0, at LE=45 -> 100
        score = float(np.clip(gap * (100.0 / self.BENCHMARK_YEARS), 0, 100))

        # Cross-country context: years to reach benchmark at current trend
        years_to_benchmark = None
        if trend and trend["annual_gain_yrs"] > 0 and gap > 0:
            years_to_benchmark = round(gap / trend["annual_gain_yrs"], 1)

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": country_iso3,
                "life_expectancy_yrs": round(latest_le, 1),
                "year": latest_year,
                "benchmark_yrs": self.BENCHMARK_YEARS,
                "gap_from_benchmark": round(gap, 2),
                "trend": trend,
                "years_to_benchmark_at_trend": years_to_benchmark,
                "category": (
                    "above-benchmark" if latest_le >= 75
                    else "near-benchmark" if latest_le >= 70
                    else "developing" if latest_le >= 60
                    else "low"
                ),
            },
        }
