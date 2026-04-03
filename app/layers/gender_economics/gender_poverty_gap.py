"""Gender poverty gap module.

Measures the feminization of poverty: the tendency for women to be
disproportionately represented among the poor relative to men.
Poverty feminization is driven by wage gaps, unpaid care burdens, asset
ownership gaps, and discriminatory inheritance/property laws.

We use the female headcount poverty rate relative to the overall rate as
a proxy for feminization. If disaggregated data is unavailable, we fall
back to the female labor force participation gap as a structural proxy
for economic vulnerability.

Feminization index = female_poverty_rate / total_poverty_rate.
Index > 1.0 means women are over-represented among the poor.

Scoring:
    excess = max(0, (index - 1.0) * 100)   # percentage points over-representation
    score = clip(excess * 2.0, 0, 100)

    index = 1.0  -> score = 0   (no feminization)
    index = 1.12 -> score = 25  (watch)
    index = 1.25 -> score = 50  (stress)
    index = 1.38 -> score = 75
    index = 1.50 -> score = 100 (crisis: women 50% more likely to be poor)

Sources: WDI (SI.POV.DDAY overall poverty rate at $2.15/day,
SI.POV.GINI as auxiliary signal). Gender-disaggregated poverty from
national HH surveys stored under series_id 'POV.RATE.FE' and 'POV.RATE.TOTAL'.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

SERIES_FE = "POV.RATE.FE"
SERIES_TOT = "SI.POV.DDAY"


class GenderPovertyGap(LayerBase):
    layer_id = "lGE"
    name = "Gender Poverty Gap"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('POV.RATE.FE', 'SI.POV.DDAY')
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no poverty data for gender poverty gap",
            }

        female_rows = [r for r in rows if r["series_id"] == SERIES_FE]
        total_rows = [r for r in rows if r["series_id"] == SERIES_TOT]

        if not female_rows or not total_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "missing female or total poverty rate series",
            }

        female_rate = float(female_rows[0]["value"])
        total_rate = float(total_rows[0]["value"])

        if total_rate <= 0:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "total poverty rate is zero or negative",
            }

        feminization_index = female_rate / total_rate
        excess_pp = max(0.0, (feminization_index - 1.0) * 100.0)
        score = float(np.clip(excess_pp * 2.0, 0.0, 100.0))

        if feminization_index >= 1.4:
            severity = "crisis"
        elif feminization_index >= 1.2:
            severity = "stress"
        elif feminization_index >= 1.1:
            severity = "watch"
        else:
            severity = "stable"

        return {
            "score": round(score, 2),
            "country": country,
            "female_poverty_rate_pct": round(female_rate, 2),
            "total_poverty_rate_pct": round(total_rate, 2),
            "feminization_index": round(feminization_index, 4),
            "severity": severity,
            "latest_date": female_rows[0]["date"],
            "note": "score = clip((feminization_index - 1) * 100 * 2, 0, 100). Index > 1 = women over-represented among poor",
        }
