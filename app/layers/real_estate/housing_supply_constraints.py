"""Housing Supply Constraints module.

Measures urban population growth vs housing supply proxy. High urban growth
with low construction activity signals supply constraint stress.

Queries:
- SP.URB.GROW: urban population growth rate
- NV.IND.TOTL.ZS: industry value added as construction proxy

Score = clip(urban_growth * 10 - industry_share + 50, 0, 100)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HousingSupplyConstraints(LayerBase):
    layer_id = "lRE"
    name = "Housing Supply Constraints"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        urban_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.URB.GROW'
            ORDER BY dp.date
            """,
            (country,),
        )

        industry_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NV.IND.TOTL.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not urban_rows or len(urban_rows) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient urban growth data for supply constraint analysis",
            }

        urban_vals = np.array([float(r["value"]) for r in urban_rows])
        urban_growth = float(np.mean(urban_vals[-3:])) if len(urban_vals) >= 3 else float(np.mean(urban_vals))

        industry_share = None
        if industry_rows and len(industry_rows) >= 1:
            industry_vals = np.array([float(r["value"]) for r in industry_rows])
            industry_share = float(np.mean(industry_vals[-3:])) if len(industry_vals) >= 3 else float(industry_vals[-1])

        # High urban growth (demand) + low industry activity (constrained supply) = stress
        if industry_share is not None:
            raw_score = urban_growth * 10 - industry_share + 50
        else:
            # Without industry data, use urban growth alone
            raw_score = urban_growth * 10 + 30

        score = float(np.clip(raw_score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "urban_growth_rate_pct": round(urban_growth, 3),
            "industry_value_added_pct": round(industry_share, 2) if industry_share is not None else None,
            "n_urban_obs": len(urban_rows),
            "n_industry_obs": len(industry_rows) if industry_rows else 0,
            "methodology": "score = clip(urban_growth * 10 - industry_share + 50, 0, 100)",
        }
