"""Housing Wealth Inequality module.

Homeownership rate gap by income quintile proxy. Uses Gini coefficient
(SI.POV.GINI) as the primary wealth inequality signal and income share
of the lowest 20% (SI.DST.FRST.20) as a bottom-quintile exclusion proxy.
High Gini with very low bottom-quintile income share implies extreme
stratification of housing wealth access.

Score = clip((gini * 0.8) - (bottom_quintile_share * 3) + 10, 0, 100)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HousingWealthInequality(LayerBase):
    layer_id = "lHO"
    name = "Housing Wealth Inequality"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        gini_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.POV.GINI'
            ORDER BY dp.date
            """,
            (country,),
        )

        quintile_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.DST.FRST.20'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not gini_rows or len(gini_rows) < 1:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient Gini data for housing wealth inequality analysis",
            }

        gini_vals = np.array([float(r["value"]) for r in gini_rows])
        gini = float(gini_vals[-1])

        bottom_quintile_share = None
        quintile_component = 0.0
        if quintile_rows and len(quintile_rows) >= 1:
            q_vals = np.array([float(r["value"]) for r in quintile_rows])
            bottom_quintile_share = float(q_vals[-1])
            quintile_component = bottom_quintile_share * 3

        raw_score = (gini * 0.8) - quintile_component + 10
        score = float(np.clip(raw_score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "gini_index": round(gini, 2),
            "bottom_quintile_income_share_pct": round(bottom_quintile_share, 2) if bottom_quintile_share is not None else None,
            "wealth_inequality_composite": round((gini * 0.8) - quintile_component, 2),
            "n_gini_obs": len(gini_rows),
            "n_quintile_obs": len(quintile_rows) if quintile_rows else 0,
            "methodology": "score = clip((gini * 0.8) - (bottom_quintile_share * 3) + 10, 0, 100)",
        }
