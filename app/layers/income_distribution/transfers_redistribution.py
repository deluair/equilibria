"""Transfers Redistribution module.

Measures redistribution effectiveness using the combination of social transfer
spending (GC.XPN.TRFT.ZS, % of expense) and Gini index (SI.POV.GINI).

Low transfer spending paired with high Gini signals failed redistribution:
the state is not correcting pre-transfer inequality.

Score = gini_score * (1 - transfer_share / 30), clipped to [0, 100].

Sources: WDI (GC.XPN.TRFT.ZS, SI.POV.GINI)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TransfersRedistribution(LayerBase):
    layer_id = "lID"
    name = "Transfers Redistribution"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        transfer_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.XPN.TRFT.ZS'
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

        if (not transfer_rows or len(transfer_rows) < 2) and (not gini_rows or len(gini_rows) < 2):
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        # Transfer share: use most recent available
        if transfer_rows:
            transfer_vals = np.array([float(r["value"]) for r in transfer_rows])
            transfer_share = float(np.mean(transfer_vals[-3:]))
            transfer_period = f"{transfer_rows[0]['date']} to {transfer_rows[-1]['date']}"
        else:
            # Default: assume low transfers if not available
            transfer_share = 5.0
            transfer_period = None

        # Gini: use most recent available
        if gini_rows:
            gini_vals = np.array([float(r["value"]) for r in gini_rows])
            gini = float(np.mean(gini_vals[-3:]))
            gini_period = f"{gini_rows[0]['date']} to {gini_rows[-1]['date']}"
        else:
            gini = 40.0
            gini_period = None

        # Gini score component: 0 at Gini=0, 100 at Gini=100
        gini_score = float(np.clip(gini, 0, 100))

        # Transfer reduction factor: higher transfers reduce the score
        # transfer_share / 30 saturates at 1.0 when transfers >= 30% of expense
        transfer_factor = 1.0 - float(np.clip(transfer_share / 30.0, 0, 1))

        score = float(np.clip(gini_score * transfer_factor, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "gini": round(gini, 2),
            "gini_period": gini_period,
            "transfer_share_pct_of_expense": round(transfer_share, 2),
            "transfer_period": transfer_period,
            "transfer_reduction_factor": round(transfer_factor, 4),
            "interpretation": (
                "low transfers + high Gini = failed redistribution; "
                "score falls as transfers increase relative to inequality"
            ),
        }
