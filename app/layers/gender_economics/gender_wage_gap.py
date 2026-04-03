"""Gender wage gap module.

Measures the ratio of female to male median earnings. A lower ratio indicates
a larger wage gap, penalizing women relative to men in labor markets.

A ratio of 1.0 (parity) -> score = 0 (no gap, stable).
A ratio of 0.60 (women earn 60% of men) -> score = 80 (stress/crisis).

Scoring:
    gap_pct = (1 - female_male_earnings_ratio) * 100
    score = clip(gap_pct * 2.5, 0, 100)

    gap = 0%   -> score = 0   (parity)
    gap = 10%  -> score = 25  (watch)
    gap = 20%  -> score = 50  (stress)
    gap = 30%  -> score = 75  (stress/crisis)
    gap = 40%  -> score = 100 (crisis)

Sources: ILO (EAR_4MTH_SEX_ECO_CUR_NB), WDI (SL.EMP.INSV.FE.ZS proxy).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

SERIES_FEMALE = "EAR_F_MEDIAN"
SERIES_MALE = "EAR_M_MEDIAN"


class GenderWageGap(LayerBase):
    layer_id = "lGE"
    name = "Gender Wage Gap"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('EAR_F_MEDIAN', 'EAR_M_MEDIAN')
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no wage data for gender wage gap",
            }

        female_rows = [r for r in rows if r["series_id"] == SERIES_FEMALE]
        male_rows = [r for r in rows if r["series_id"] == SERIES_MALE]

        if not female_rows or not male_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "missing female or male earnings series",
            }

        female_val = float(female_rows[0]["value"])
        male_val = float(male_rows[0]["value"])

        if male_val <= 0:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "male earnings value is zero or negative",
            }

        ratio = female_val / male_val
        gap_pct = (1.0 - ratio) * 100.0
        score = float(np.clip(gap_pct * 2.5, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "female_earnings": round(female_val, 2),
            "male_earnings": round(male_val, 2),
            "female_male_ratio": round(ratio, 4),
            "wage_gap_pct": round(gap_pct, 2),
            "latest_date": female_rows[0]["date"],
            "note": "score = clip((1 - female/male earnings ratio) * 100 * 2.5, 0, 100)",
        }
