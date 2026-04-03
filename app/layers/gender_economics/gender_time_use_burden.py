"""Gender time use burden module.

Measures the disparity in unpaid care and domestic work between women and men.
Women typically spend 2-10x more hours on unpaid work, limiting their labor
market participation and economic autonomy.

burden_ratio = female_unpaid_hours / male_unpaid_hours
A ratio of 1.0 (parity) -> score = 0.
A ratio of 3.0 (women do 3x more) -> score = ~67.
A ratio of 5.0+ -> score = 100 (crisis).

Scoring:
    excess_ratio = max(0, ratio - 1.0)
    score = clip(excess_ratio * 25, 0, 100)

    ratio = 1.0 -> score = 0   (parity)
    ratio = 2.0 -> score = 25  (watch)
    ratio = 3.0 -> score = 50  (stress)
    ratio = 4.0 -> score = 75
    ratio = 5.0 -> score = 100 (crisis)

Sources: ILO time use surveys (SDG_T533_SEX_RT time burden ratio female/male).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

SERIES_RATIO = "SDG_T533_SEX_RT"


class GenderTimeUseBurden(LayerBase):
    layer_id = "lGE"
    name = "Gender Time Use Burden"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SDG_T533_SEX_RT'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no time use burden data (SDG_T533_SEX_RT)",
            }

        ratio = float(rows[0]["value"])
        if ratio <= 0:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "invalid time burden ratio value",
            }

        excess_ratio = max(0.0, ratio - 1.0)
        score = float(np.clip(excess_ratio * 25.0, 0.0, 100.0))

        if ratio >= 4:
            severity = "crisis"
        elif ratio >= 3:
            severity = "stress"
        elif ratio >= 2:
            severity = "watch"
        else:
            severity = "stable"

        return {
            "score": round(score, 2),
            "country": country,
            "female_male_unpaid_work_ratio": round(ratio, 3),
            "severity": severity,
            "latest_date": rows[0]["date"],
            "n_obs": len(rows),
            "note": "score = clip((ratio - 1) * 25, 0, 100). Ratio = female/male unpaid hours. Series: SDG_T533_SEX_RT",
        }
