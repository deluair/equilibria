"""Land Degradation: arable land per capita trend as a proxy for land pressure.

Declining arable land per capita signals a shrinking agricultural resource base
relative to population growth, indicating increasing land pressure and potential
food security risks tied to land degradation and unsustainable use.

Methodology:
    Retrieve AG.LND.ARBL.HA.PC (arable land hectares per person).
    Fit linear OLS trend over time:
        slope = d(arable_ha_pc)/dt
    Score derived from the slope scaled to a 0-100 range:
        score = clip(-slope * 10000, 0, 100)
    Negative slope (declining land per capita) increases the score.
    If slope is positive (improving), score approaches 0.

References:
    Foley, J. et al. (2011). "Solutions for a cultivated planet." Nature, 478, 337-342.
    FAO (2022). The State of the World's Land and Water Resources. FAO, Rome.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class LandDegradation(LayerBase):
    layer_id = "lSU"
    name = "Land Degradation"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'AG.LND.ARBL.HA.PC'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient arable land per capita data"}

        land_by_year: dict[str, float] = {}
        for r in rows:
            yr = r["date"][:4] if isinstance(r["date"], str) else str(r["date"])
            land_by_year[yr] = float(r["value"])

        sorted_years = sorted(land_by_year.keys())
        years = np.array([int(y) for y in sorted_years])
        land = np.array([land_by_year[y] for y in sorted_years])

        slope, _, r_value, _, _ = stats.linregress(years - years[0], land)
        score = float(np.clip(-slope * 10000, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_years": len(sorted_years),
            "arable_land_ha_pc_latest": round(float(land[-1]), 4),
            "arable_land_ha_pc_first": round(float(land[0]), 4),
            "trend_slope": round(float(slope), 6),
            "r_squared": round(float(r_value ** 2), 4),
            "trend_direction": "declining" if slope < 0 else "stable_or_improving",
            "year_range": [sorted_years[0], sorted_years[-1]],
        }
