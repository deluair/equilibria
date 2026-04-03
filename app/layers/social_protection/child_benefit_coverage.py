"""Child Benefit Coverage module.

Child and family benefit proxy: education spending combined with child poverty.

Queries:
- 'SE.XPD.TOTL.GD.ZS' (government expenditure on education as % of GDP)
- 'SI.POV.DDAY' (poverty headcount ratio at $2.15/day, % of population)

Low education spend multiplied by high poverty signals inadequate child benefit coverage.

Score = clip(max(0, 8 - edu_spend) * poverty_rate / 5, 0, 100)

Sources: WDI (SE.XPD.TOTL.GD.ZS, SI.POV.DDAY)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ChildBenefitCoverage(LayerBase):
    layer_id = "lSP"
    name = "Child Benefit Coverage"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        edu_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SE.XPD.TOTL.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        pov_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.POV.DDAY'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not edu_rows or not pov_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        edu_vals = [float(r["value"]) for r in edu_rows if r["value"] is not None]
        pov_vals = [float(r["value"]) for r in pov_rows if r["value"] is not None]

        if not edu_vals or not pov_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        edu_spend = float(np.mean(edu_vals))
        poverty_rate = float(np.mean(pov_vals))

        edu_gap = max(0.0, 8.0 - edu_spend)
        score = float(np.clip(edu_gap * poverty_rate / 5.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "edu_spend_pct_gdp": round(edu_spend, 2),
            "poverty_headcount_pct": round(poverty_rate, 2),
            "edu_gap": round(edu_gap, 2),
            "n_obs_edu": len(edu_vals),
            "n_obs_poverty": len(pov_vals),
            "interpretation": (
                "Low government education spending combined with high poverty "
                "indicates inadequate child and family benefit coverage."
            ),
            "_series": ["SE.XPD.TOTL.GD.ZS", "SI.POV.DDAY"],
            "_source": "WDI",
        }
