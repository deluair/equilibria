"""Rent Seeking module.

Measures rent-seeking intensity as the interaction of resource rents
and corruption (Tollison 1982, Krueger 1974).

Score = resource_rents_pct_gdp * (1 - normalized_control_of_corruption),
clipped to [0, 100].

- High resource rents + weak corruption control = intense rent-seeking
- Low rents or strong institutions = low score

Sources: WDI (NY.GDP.TOTL.RT.ZS), WGI (CC.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RentSeeking(LayerBase):
    layer_id = "lGT"
    name = "Rent Seeking"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        # Query total resource rents (% of GDP)
        rent_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.TOTL.RT.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        # Query control of corruption (WGI, estimate, roughly -2.5 to +2.5)
        cc_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'CC.EST'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not rent_rows or not cc_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data: need NY.GDP.TOTL.RT.ZS and CC.EST",
            }

        resource_rents = float(np.mean([float(r["value"]) for r in rent_rows]))
        cc_est = float(np.mean([float(r["value"]) for r in cc_rows]))

        # Normalize CC.EST from [-2.5, +2.5] to [0, 1] where 1 = best control
        cc_normalized = float(np.clip((cc_est + 2.5) / 5.0, 0.0, 1.0))

        # Rent-seeking intensity: high rents + weak control
        raw_score = resource_rents * (1.0 - cc_normalized)
        score = float(np.clip(raw_score, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "resource_rents_pct_gdp": round(resource_rents, 3),
            "cc_est_mean": round(cc_est, 4),
            "cc_normalized": round(cc_normalized, 4),
            "n_rent_obs": len(rent_rows),
            "n_cc_obs": len(cc_rows),
            "interpretation": (
                "high rent-seeking intensity" if score > 50
                else "moderate rent-seeking" if score > 25
                else "low rent-seeking intensity"
            ),
        }
