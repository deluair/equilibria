"""Quintile Income Ratio module.

Measures top-to-bottom quintile income ratio as a proxy for extreme inequality.
Queries top 10% income share (SI.DST.10TH.10) and bottom 10% share
(SI.DST.FRST.10). When those are unavailable, falls back to Gini as a proxy.

High ratio = extreme inequality concentration at the top.
Score = clip((ratio - 5) * 10, 0, 100).

Sources: WDI (SI.DST.10TH.10, SI.DST.FRST.10, SI.POV.GINI)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class QuintileIncomeRatio(LayerBase):
    layer_id = "lID"
    name = "Quintile Income Ratio"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        top_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.DST.10TH.10'
            ORDER BY dp.date
            """,
            (country,),
        )

        bot_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.DST.FRST.10'
            ORDER BY dp.date
            """,
            (country,),
        )

        ratio = None
        method = "direct"
        period = None

        if top_rows and bot_rows and len(top_rows) >= 3 and len(bot_rows) >= 3:
            top_map = {r["date"]: float(r["value"]) for r in top_rows}
            bot_map = {r["date"]: float(r["value"]) for r in bot_rows}
            common = sorted(set(top_map) & set(bot_map))
            if len(common) >= 3:
                # Use most recent 5 observations for ratio
                recent = common[-5:]
                top_vals = np.array([top_map[d] for d in recent])
                bot_vals = np.array([bot_map[d] for d in recent])
                bot_nonzero = np.where(bot_vals > 0, bot_vals, np.nan)
                ratios = top_vals / bot_nonzero
                valid = ratios[np.isfinite(ratios)]
                if len(valid) > 0:
                    ratio = float(np.mean(valid))
                    period = f"{recent[0]} to {recent[-1]}"

        if ratio is None:
            # Fallback: derive proxy from Gini
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
            if not gini_rows or len(gini_rows) < 3:
                return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}
            gini_vals = np.array([float(r["value"]) for r in gini_rows])
            period = f"{gini_rows[0]['date']} to {gini_rows[-1]['date']}"
            gini_mean = float(np.mean(gini_vals[-5:]))
            # Approximate ratio from Gini: empirical relationship
            # For Gini ~ 0.3, ratio ~ 6; for Gini ~ 0.6, ratio ~ 20
            ratio = 2 + gini_mean * 0.3
            method = "gini_proxy"

        score = float(np.clip((ratio - 5) * 10, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "top_bottom_ratio": round(ratio, 2),
            "method": method,
            "period": period,
            "interpretation": "ratio > 5 = top 10% earns 5x bottom 10%; higher = more extreme inequality",
        }
