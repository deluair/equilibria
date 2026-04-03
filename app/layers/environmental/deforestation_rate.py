"""Deforestation rate: forest area trend as environmental stress indicator.

Queries World Bank WDI series AG.LND.FRST.ZS (forest area as % of land area)
over time for the target country. Computes an OLS slope to measure the trend
in forest cover. A declining trend signals deforestation pressure and
environmental degradation.

Score = clip(-slope * 100 + 50, 0, 100):
  - Stable or increasing forest cover -> score near 50 or below
  - Steep decline -> score approaches 100 (maximum stress)

Sources: World Bank WDI (AG.LND.FRST.ZS)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class DeforestationRate(LayerBase):
    layer_id = "l9"
    name = "Deforestation Rate"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3")

        if not country:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "country_iso3 required",
            }

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'AG.LND.FRST.ZS'
              AND ds.country_iso3 = ?
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient forest area data",
            }

        years = np.array([int(r["date"][:4]) for r in rows], dtype=float)
        values = np.array([r["value"] for r in rows], dtype=float)

        mask = ~np.isnan(values)
        years, values = years[mask], values[mask]

        if len(years) < 3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient non-null forest area data",
            }

        slope, _, r_value, p_value, _ = linregress(years, values)

        score = float(np.clip(-slope * 100 + 50, 0, 100))

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": country,
                "series_id": "AG.LND.FRST.ZS",
                "n_obs": int(len(years)),
                "latest_year": int(years[-1]),
                "latest_forest_pct": round(float(values[-1]), 2),
                "earliest_forest_pct": round(float(values[0]), 2),
                "slope_pct_per_year": round(float(slope), 4),
                "r_squared": round(float(r_value ** 2), 4),
                "p_value": round(float(p_value), 4),
                "trend": (
                    "declining" if slope < -0.01 and p_value < 0.10
                    else "increasing" if slope > 0.01 and p_value < 0.10
                    else "stable"
                ),
            },
        }
