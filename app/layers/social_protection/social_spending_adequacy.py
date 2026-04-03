"""Social Spending Adequacy module.

Total social spending as % of GDP vs need, proxied by inequality.

Queries:
- 'GC.XPN.TOTL.GD.ZS' (general government total expenditure as % of GDP)
- 'SI.POV.GINI' (Gini index)

Low government spending combined with high inequality signals social spending inadequacy.

Score = clip(gini / 100 * max(0, 25 - total_spend) * 4, 0, 100)

Sources: WDI (GC.XPN.TOTL.GD.ZS, SI.POV.GINI)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SocialSpendingAdequacy(LayerBase):
    layer_id = "lSP"
    name = "Social Spending Adequacy"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        spend_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.XPN.TOTL.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        gini_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.POV.GINI'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not spend_rows or not gini_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        spend_vals = [float(r["value"]) for r in spend_rows if r["value"] is not None]
        gini_vals = [float(r["value"]) for r in gini_rows if r["value"] is not None]

        if not spend_vals or not gini_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        total_spend = float(np.mean(spend_vals))
        gini = float(np.mean(gini_vals))

        spend_gap = max(0.0, 25.0 - total_spend)
        score = float(np.clip((gini / 100.0) * spend_gap * 4.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "govt_expenditure_pct_gdp": round(total_spend, 2),
            "gini_index": round(gini, 2),
            "spend_gap": round(spend_gap, 2),
            "n_obs_spend": len(spend_vals),
            "n_obs_gini": len(gini_vals),
            "interpretation": (
                "Low government expenditure relative to high inequality signals "
                "inadequate social spending to address redistribution needs."
            ),
            "_series": ["GC.XPN.TOTL.GD.ZS", "SI.POV.GINI"],
            "_source": "WDI",
        }
