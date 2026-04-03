"""Poverty Decomposition module.

Analyzes poverty headcount and gap to identify depth of poverty stress.
High headcount combined with a large poverty gap indicates deep, severe poverty.

Sources: WDI (SI.POV.DDAY, SI.POV.GAPS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PovertyDecomposition(LayerBase):
    layer_id = "lWE"
    name = "Poverty Decomposition"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        headcount_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.POV.DDAY'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        gap_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.POV.GAPS'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not headcount_rows and not gap_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no poverty data available",
            }

        headcount = float(headcount_rows[0]["value"]) if headcount_rows else None
        headcount_date = headcount_rows[0]["date"] if headcount_rows else None
        gap = float(gap_rows[0]["value"]) if gap_rows else None
        gap_date = gap_rows[0]["date"] if gap_rows else None

        # Compute stress components
        # Headcount: % below $2.15/day -> 0-100 scale directly
        headcount_score = float(np.clip(headcount, 0, 100)) if headcount is not None else 0.0
        # Gap: depth of poverty as % of poverty line -> amplifies headcount
        gap_score = float(np.clip(gap * 2, 0, 100)) if gap is not None else 0.0

        if headcount is not None and gap is not None:
            score = 0.6 * headcount_score + 0.4 * gap_score
        elif headcount is not None:
            score = headcount_score
        else:
            score = gap_score

        # Trend for headcount
        headcount_trend = None
        if len(headcount_rows) > 1:
            vals = [float(r["value"]) for r in headcount_rows]
            headcount_trend = round(float(vals[0] - vals[-1]), 2)

        return {
            "score": round(score, 1),
            "country": country,
            "headcount_pct": round(headcount, 2) if headcount is not None else None,
            "headcount_date": headcount_date,
            "poverty_gap_pct": round(gap, 2) if gap is not None else None,
            "poverty_gap_date": gap_date,
            "headcount_trend": headcount_trend,
            "method": "score = 0.6 * headcount + 0.4 * (gap * 2), clipped to [0, 100]",
            "reference": "Foster-Greer-Thorbecke 1984; WDI $2.15/day threshold",
        }
