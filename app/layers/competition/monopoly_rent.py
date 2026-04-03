"""Monopoly Rent module.

Estimates rent extraction potential as the interaction of:
1. Natural resource rents (% of GDP): NY.GDP.TOTL.RT.ZS
   High resource rents create concentrated income streams that incumbents
   (state or private) can capture.
2. Corruption (Control of Corruption WGI estimate): CC.EST
   Poor governance amplifies rent extraction; corrupt environments
   allow monopolists to sustain rents through regulatory capture.

Monopoly rent stress = resource_rents * corruption_weight.
  corruption_weight = clip(1 - cc_est, 0.1, 2.0)
  where CC.EST ranges approximately -2.5 (worst) to +2.5 (best).

Score = clip(resource_rents * corruption_weight * 5, 0, 100).

Sources: WDI (NY.GDP.TOTL.RT.ZS), World Governance Indicators (CC.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MonopolyRent(LayerBase):
    layer_id = "lCO"
    name = "Monopoly Rent"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

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

        if not rent_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no resource rent data"}

        def latest_value(rows) -> float | None:
            for r in rows:
                if r["value"] is not None:
                    try:
                        return float(r["value"])
                    except (TypeError, ValueError):
                        pass
            return None

        resource_rents = latest_value(rent_rows)
        cc_est = latest_value(cc_rows)

        if resource_rents is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "missing resource rent value"}

        # cc_est: higher = better governance; missing -> neutral (0)
        cc = cc_est if cc_est is not None else 0.0
        # Weight: poor governance multiplies rent extraction risk
        corruption_weight = float(np.clip(1.0 - cc, 0.1, 3.0))

        score = float(np.clip(resource_rents * corruption_weight * 5, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "resource_rents_pct_gdp": round(resource_rents, 3),
            "control_of_corruption_est": round(cc, 3),
            "corruption_weight": round(corruption_weight, 3),
            "interpretation": (
                "low rent extraction" if score < 33
                else "moderate rent risk" if score < 66
                else "high monopoly rent extraction"
            ),
            "reference": (
                "Sachs & Warner (1995): resource curse; Kaufmann et al. (2010): WGI"
            ),
        }
