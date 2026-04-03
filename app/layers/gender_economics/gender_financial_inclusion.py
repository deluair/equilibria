"""Gender financial inclusion module.

Measures the gap between female and male account ownership at financial
institutions or mobile money providers. Financial exclusion limits women's
ability to save, invest, and access credit, deepening economic inequality.

gap_pp = male_account_pct - female_account_pct
Scoring:
    score = clip(gap_pp * 2.5, 0, 100)

    gap = 0pp  -> score = 0   (parity)
    gap = 10pp -> score = 25  (watch)
    gap = 20pp -> score = 50  (stress)
    gap = 30pp -> score = 75
    gap = 40pp -> score = 100 (crisis)

Sources: WDI (FX.OWN.TOTL.FE.ZS female, FX.OWN.TOTL.MA.ZS male account ownership).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

SERIES_F = "FX.OWN.TOTL.FE.ZS"
SERIES_M = "FX.OWN.TOTL.MA.ZS"


class GenderFinancialInclusion(LayerBase):
    layer_id = "lGE"
    name = "Gender Financial Inclusion"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('FX.OWN.TOTL.FE.ZS', 'FX.OWN.TOTL.MA.ZS')
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no financial account ownership data",
            }

        female_rows = [r for r in rows if r["series_id"] == SERIES_F]
        male_rows = [r for r in rows if r["series_id"] == SERIES_M]

        if not female_rows or not male_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "missing female or male account ownership series",
            }

        female_pct = float(female_rows[0]["value"])
        male_pct = float(male_rows[0]["value"])
        gap_pp = male_pct - female_pct
        score = float(np.clip(gap_pp * 2.5, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "female_account_pct": round(female_pct, 2),
            "male_account_pct": round(male_pct, 2),
            "account_gap_pp": round(gap_pp, 2),
            "latest_date": female_rows[0]["date"],
            "note": "score = clip(gap_pp * 2.5, 0, 100). Series: FX.OWN.TOTL.FE/MA.ZS",
        }
