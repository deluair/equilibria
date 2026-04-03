"""Renewable Energy Transition: share level and adoption speed of renewable electricity.

Assesses the pace and progress of the renewable energy transition using the
World Bank indicator for renewable electricity share. Combines the current
share gap from 100% with the trend slope to score transition stress.

Methodology:
    level_gap = 100 - share_latest  (0 = fully renewable, 100 = none)
    Fit linear OLS on share over time:
        slope = d(share)/dt  (pct per year)
        slope_max = 3.0 pct/yr (aspirational rapid transition)
        slope_gap = clip(100 - slope / slope_max * 100, 0, 100)
    score = level_gap * 0.7 + slope_gap * 0.3

References:
    IRENA (2023). World Energy Transitions Outlook 2023. IRENA, Abu Dhabi.
    Jacobson, M.Z. et al. (2017). "100% clean and renewable wind, water, and
        sunlight all-sector energy roadmaps." Joule, 1(1), 108-121.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase

_SLOPE_MAX = 3.0  # percentage points per year (aspirational)


class RenewableEnergyTransition(LayerBase):
    layer_id = "lSU"
    name = "Renewable Energy Transition"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'EG.ELC.RNEW.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 4:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient renewable electricity share data"}

        share_by_year: dict[str, float] = {}
        for r in rows:
            yr = r["date"][:4] if isinstance(r["date"], str) else str(r["date"])
            share_by_year[yr] = float(r["value"])

        sorted_years = sorted(share_by_year.keys())
        years = np.array([int(y) for y in sorted_years])
        shares = np.array([share_by_year[y] for y in sorted_years])

        share_latest = float(shares[-1])
        level_gap = float(np.clip(100.0 - share_latest, 0, 100))

        slope, _, _, _, _ = stats.linregress(years - years[0], shares)
        slope_gap = float(np.clip(100.0 - slope / _SLOPE_MAX * 100, 0, 100))

        score = float(np.clip(level_gap * 0.7 + slope_gap * 0.3, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_years": len(sorted_years),
            "renewable_share_pct": round(share_latest, 2),
            "level_gap": round(level_gap, 2),
            "trend_slope_pct_per_yr": round(float(slope), 4),
            "slope_gap": round(slope_gap, 2),
            "year_range": [sorted_years[0], sorted_years[-1]],
            "transition_status": (
                "advanced" if share_latest >= 70 else
                "progressing" if share_latest >= 30 else
                "early_stage"
            ),
        }
