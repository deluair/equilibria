"""Middle Class Share module.

Estimates middle class income share (proxy for the middle 60%) using
bottom quintile share (SI.DST.FRST.20) and Gini index (SI.POV.GINI).

Middle class squeeze: when Gini rises while the bottom 20% share falls,
the middle is being compressed from both ends.

Score based on compression of the middle class relative to historical norms.

Sources: WDI (SI.DST.FRST.20, SI.POV.GINI)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MiddleClassShare(LayerBase):
    layer_id = "lID"
    name = "Middle Class Share"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        bottom_rows = await db.fetch_all(
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

        if (not bottom_rows or len(bottom_rows) < 3) and (not gini_rows or len(gini_rows) < 3):
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        results = {"country": country}

        # Estimate middle 60% share: 100 - top 20% - bottom 20%
        # Top 20% share proxy from Gini: top_20 ~ 40 + Gini * 0.5 (empirical approx)
        middle_share = None
        period = None

        if bottom_rows and gini_rows and len(bottom_rows) >= 3 and len(gini_rows) >= 3:
            bot_map = {r["date"]: float(r["value"]) for r in bottom_rows}
            gini_map = {r["date"]: float(r["value"]) for r in gini_rows}
            common = sorted(set(bot_map) & set(gini_map))
            if len(common) >= 3:
                recent = common[-5:]
                bot_vals = np.array([bot_map[d] for d in recent])
                gini_vals = np.array([gini_map[d] for d in recent])
                # Approximate top 20% share from Gini
                top20_proxy = 40 + gini_vals * 0.5
                middle_shares = np.clip(100 - top20_proxy - bot_vals, 0, 100)
                middle_share = float(np.mean(middle_shares))
                period = f"{recent[0]} to {recent[-1]}"

                # Trend: is middle share declining?
                if len(common) >= 5:
                    all_bot = np.array([bot_map[d] for d in common])
                    all_gini = np.array([gini_map[d] for d in common])
                    all_top20 = 40 + all_gini * 0.5
                    all_middle = np.clip(100 - all_top20 - all_bot, 0, 100)
                    t = np.arange(len(all_middle))
                    slope = float(np.polyfit(t, all_middle, 1)[0])
                    results["middle_share_trend_per_year"] = round(slope, 4)

        elif gini_rows and len(gini_rows) >= 3:
            gini_vals = np.array([float(r["value"]) for r in gini_rows[-5:]])
            gini_mean = float(np.mean(gini_vals))
            # Rough middle 60% from Gini: lower Gini -> higher middle share
            middle_share = max(20, 60 - gini_mean * 0.4)
            period = f"{gini_rows[0]['date']} to {gini_rows[-1]['date']}"
            results["method"] = "gini_proxy"

        if middle_share is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        results["middle_class_share_pct"] = round(middle_share, 2)
        results["period"] = period

        # Score: middle class healthy around 50%, compressed below 40%
        # Score = how far below 50% the middle share is, clipped
        score = float(np.clip((50 - middle_share) * 2, 0, 100))

        return {"score": round(score, 1), **results}
