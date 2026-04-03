"""Gender legal rights module.

Measures the degree of legal parity between women and men across dimensions
such as workplace protections, property rights, inheritance, marriage laws,
and access to justice. The World Bank Women, Business and the Law (WBL)
index scores countries 0-100 where 100 = full legal parity.

We invert the WBL index so that higher scores indicate worse legal conditions:
    score = 100 - wbl_index

    WBL = 100 -> score = 0   (full parity, STABLE)
    WBL = 75  -> score = 25  (watch)
    WBL = 50  -> score = 50  (stress)
    WBL = 25  -> score = 75  (crisis)
    WBL = 0   -> score = 100 (complete legal exclusion)

Sources: World Bank Women Business and the Law index (SG.LAW.INDX).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

SERIES = "SG.LAW.INDX"


class GenderLegalRights(LayerBase):
    layer_id = "lGE"
    name = "Gender Legal Rights"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SG.LAW.INDX'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no Women Business and the Law index data (SG.LAW.INDX)",
            }

        wbl = float(rows[0]["value"])
        wbl = float(np.clip(wbl, 0.0, 100.0))
        score = round(100.0 - wbl, 2)

        # Trend
        trend = "insufficient data"
        if len(rows) >= 3:
            vals = np.array([float(r["value"]) for r in sorted(rows[:10], key=lambda r: r["date"])], dtype=float)
            slope = float(np.polyfit(np.arange(len(vals), dtype=float), vals, 1)[0])
            trend = "improving" if slope > 0.5 else "deteriorating" if slope < -0.5 else "stable"

        if wbl >= 90:
            category = "near_parity"
        elif wbl >= 70:
            category = "partial_parity"
        elif wbl >= 50:
            category = "significant_gaps"
        else:
            category = "severe_inequality"

        return {
            "score": score,
            "country": country,
            "wbl_index": round(wbl, 2),
            "legal_parity_category": category,
            "trend_wbl": trend,
            "latest_date": rows[0]["date"],
            "n_obs": len(rows),
            "note": "score = 100 - WBL_index. WBL=100 is full legal parity. Series: SG.LAW.INDX",
        }
