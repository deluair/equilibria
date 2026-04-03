"""Financial Inclusion module.

Bank account ownership as a proxy for financial inclusion.
Queries 'FX.OWN.TOTL.ZS' (account ownership % adults 15+, WDI).

Score = max(0, 70 - ownership_pct) * 1.43
Below 70% account ownership => financial exclusion stress.
At 0% ownership => score 100. At 70%+ => score 0.

Source: World Bank Global Findex / WDI FX.OWN.TOTL.ZS
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FinancialInclusion(LayerBase):
    layer_id = "lDF"
    name = "Financial Inclusion"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FX.OWN.TOTL.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        ownership_pct = float(np.nanmean(values))
        score = float(np.clip(max(0.0, 70.0 - ownership_pct) * 1.43, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "account_ownership_pct": round(ownership_pct, 2),
            "n_obs": len(values),
            "latest_year": rows[0]["date"][:4] if rows else None,
            "threshold_pct": 70.0,
            "note": "Score 0 = full inclusion (>=70% banked). Score 100 = complete exclusion.",
            "_citation": "World Bank WDI: FX.OWN.TOTL.ZS (Global Findex)",
        }
