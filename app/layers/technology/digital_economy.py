"""Digital Economy module.

Digital penetration: internet users + telephone lines (broadband proxy).

Score = max(0, 80 - internet_pct) * 1.25
Lagging digital adoption = higher stress score.

Sources: WDI (IT.NET.USER.ZS, IT.MLT.MAIN.P2)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DigitalEconomy(LayerBase):
    layer_id = "lTE"
    name = "Digital Economy"

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
        phone_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'IT.MLT.MAIN.P2'
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

        phone_pct = None
        if phone_rows:
            phone_vals = np.array([float(r["value"]) for r in phone_rows])
            phone_pct = float(phone_vals[-1])

        score = float(np.clip(max(0.0, 80.0 - internet_pct) * 1.25, 0.0, 100.0))

        result = {
            "score": round(score, 1),
            "country": country,
            "internet_users_pct_latest": round(internet_pct, 2),
            "internet_n_obs": len(internet_rows),
            "period": f"{internet_rows[0]['date']} to {internet_rows[-1]['date']}",
            "interpretation": "score rises as internet penetration falls below 80%",
        }
        if phone_pct is not None:
            result["telephone_lines_per100_latest"] = round(phone_pct, 2)

        return result
