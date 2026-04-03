"""Auction Theory module.

Measures procurement/fiscal efficiency via interest payments as share
of total government expenditure (Milgrom & Weber 1982, Klemperer 2004).

High interest/spend ratio indicates the government is allocating a
disproportionate share of expenditure to debt servicing, a symptom of
poor debt auction design, weak fiscal credibility, or adverse selection
in sovereign debt markets.

Both series in current LCU (CN); ratio is dimensionless.

Sources: WDI (GC.XPN.INTP.CN, GC.XPN.TOTL.CN)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# Interest/expenditure ratio above which fiscal stress is material
_INTEREST_RATIO_HIGH = 0.20   # 20% of spend on interest = high stress
_INTEREST_RATIO_LOW = 0.05    # 5% = low/normal


class AuctionTheory(LayerBase):
    layer_id = "lGT"
    name = "Auction Theory"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        interest_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.XPN.INTP.CN'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        spend_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.XPN.TOTL.CN'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        if not interest_rows or not spend_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data: need GC.XPN.INTP.CN and GC.XPN.TOTL.CN",
            }

        # Align by date for ratio computation
        interest_map = {r["date"]: float(r["value"]) for r in interest_rows}
        spend_map = {r["date"]: float(r["value"]) for r in spend_rows}
        common_dates = sorted(set(interest_map) & set(spend_map))

        if not common_dates:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no overlapping dates between interest and expenditure series",
            }

        ratios = []
        for d in common_dates:
            s = spend_map[d]
            if abs(s) > 1e-10:
                ratios.append(interest_map[d] / s)

        if not ratios:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "zero expenditure values prevent ratio computation",
            }

        mean_ratio = float(np.mean(ratios))
        trend = float(np.polyfit(np.arange(len(ratios), dtype=float), ratios, 1)[0]) if len(ratios) >= 3 else 0.0

        # Score: linearly scale ratio from low (0) to high (100)
        base_score = float(
            np.clip(
                (mean_ratio - _INTEREST_RATIO_LOW) / (_INTEREST_RATIO_HIGH - _INTEREST_RATIO_LOW) * 100.0,
                0.0,
                100.0,
            )
        )

        # Small trend penalty: rising ratio adds up to 10 points
        trend_penalty = float(np.clip(trend * 500.0, 0.0, 10.0))
        score = float(np.clip(base_score + trend_penalty, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "interest_to_expenditure_ratio": round(mean_ratio, 4),
            "ratio_trend": round(trend, 6),
            "n_common_obs": len(ratios),
            "period": f"{common_dates[0]} to {common_dates[-1]}",
            "interpretation": (
                "severe fiscal stress from interest burden" if score > 60
                else "elevated interest burden" if score > 30
                else "manageable debt servicing costs"
            ),
        }
