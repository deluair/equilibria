"""Network Effects module.

Digital network size: combined internet penetration + mobile subscriptions.

Score = max(0, 100 - (internet_pct + mobile_per100/10) / 2)
Low combined penetration = network effects threshold not reached.

Sources: WDI (IT.NET.USER.ZS, IT.CEL.SETS.P2)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class NetworkEffects(LayerBase):
    layer_id = "lTE"
    name = "Network Effects"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        internet_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'IT.NET.USER.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )
        mobile_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'IT.CEL.SETS.P2'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not internet_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no internet penetration data",
            }

        internet_vals = np.array([float(r["value"]) for r in internet_rows])
        internet_pct = float(internet_vals[-1])

        mobile_per100 = None
        if mobile_rows:
            mobile_vals = np.array([float(r["value"]) for r in mobile_rows])
            mobile_per100 = float(mobile_vals[-1])

        if mobile_per100 is not None:
            combined = (internet_pct + mobile_per100 / 10.0) / 2.0
        else:
            combined = internet_pct

        score = float(np.clip(max(0.0, 100.0 - combined), 0.0, 100.0))

        result = {
            "score": round(score, 1),
            "country": country,
            "internet_pct_latest": round(internet_pct, 2),
            "combined_adoption_score": round(combined, 2),
            "internet_n_obs": len(internet_rows),
            "interpretation": "low combined adoption = network effects threshold not reached",
        }
        if mobile_per100 is not None:
            result["mobile_subscriptions_per100_latest"] = round(mobile_per100, 2)
            result["mobile_n_obs"] = len(mobile_rows)

        return result
