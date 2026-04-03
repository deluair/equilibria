"""Youth bulge: youth dependency and infrastructure pressure.

A youth bulge occurs when a large cohort of young people (0-14) enters the
population relative to the working-age group (15-64). High youth dependency
ratios impose simultaneous pressure on education systems, healthcare, housing,
and the labor market as cohorts age into the workforce.

The literature distinguishes between the "demographic dividend" (youth bulge ->
labor force expansion -> growth if productive) and the "youth bulge hypothesis"
(Urdal 2006, Cincotta et al. 2003): when youth cannot be absorbed productively,
high dependency ratios correlate with social unrest, unemployment, and strained
public services.

Score formula: clip(ratio - 30, 0, 70) * 1.43 -> range [0, 100].
At ratio = 30: score = 0 (STABLE, normal).
At ratio = 60: score = 43 (WATCH/STRESS boundary).
At ratio = 79+: score = 70 * 1.43 = 100 (CRISIS).

References:
    Urdal, H. (2006). A Clash of Generations? Youth Bulges and Political Violence.
        International Studies Quarterly, 50(3), 607-629.
    Cincotta, R., Engelman, R. & Anastasion, D. (2003). The Security Demographic.
        Population Action International.
    Bloom, D., Canning, D. & Sevilla, J. (2003). The Demographic Dividend.
        RAND Corporation.

Series: SP.POP.DPND.YG (age dependency ratio, young, % of working-age).
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class YouthBulge(LayerBase):
    layer_id = "l17"
    name = "Youth Bulge"
    weight = 0.20

    # Thresholds
    BASELINE_RATIO = 30.0   # below this: no excess pressure
    MAX_EXCESS = 70.0       # excess above baseline that maps to score=100
    SCALE = 100.0 / MAX_EXCESS  # 1.4286 per excess point

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
            WHERE ds.series_id = 'SP.POP.DPND.YG'
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
                "error": f"no youth dependency data for {country_iso3}",
            }

        years = [int(r["date"][:4]) for r in rows]
        values = [float(r["value"]) for r in rows]

        latest_ratio = values[-1]
        latest_year = years[-1]

        # Trend
        trend = None
        if len(values) >= 5:
            yr_arr = np.array(years)
            val_arr = np.array(values)
            slope, intercept, r, p, se = stats.linregress(yr_arr, val_arr)
            trend = {
                "annual_change": round(float(slope), 4),
                "r_squared": round(float(r ** 2), 4),
                "p_value": round(float(p), 6),
                "n_obs": len(values),
                "declining": slope < 0,  # declining = demographic transition underway
            }

        # Score
        excess = max(0.0, latest_ratio - self.BASELINE_RATIO)
        score = float(np.clip(excess * self.SCALE, 0, 100))

        # Dividend window indicator: bulge is transitioning if trend strongly declining
        dividend_window = (
            trend is not None
            and trend["declining"]
            and latest_ratio > 40
            and trend["annual_change"] < -0.5
        )

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": country_iso3,
                "youth_dependency_ratio": round(latest_ratio, 2),
                "year": latest_year,
                "baseline_ratio": self.BASELINE_RATIO,
                "excess_above_baseline": round(excess, 2),
                "trend": trend,
                "demographic_dividend_window": dividend_window,
                "bulge_stage": (
                    "low" if latest_ratio < 40
                    else "moderate" if latest_ratio < 55
                    else "high" if latest_ratio < 70
                    else "extreme"
                ),
            },
        }
