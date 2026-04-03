"""Atkinson Inequality Index module.

Uses Gini coefficient as a proxy for the Atkinson index of inequality.
Higher Gini implies greater inequality and welfare stress.

Score = clip(gini_latest - 25, 0, 75) * 1.33

Sources: WDI (SI.POV.GINI)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class AtkinsonInequality(LayerBase):
    layer_id = "lWE"
    name = "Atkinson Inequality"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.POV.GINI'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no Gini data available",
            }

        gini_latest = float(rows[0]["value"])
        gini_date = rows[0]["date"]

        all_values = np.array([float(r["value"]) for r in rows])
        gini_mean = float(np.mean(all_values))
        gini_trend = float(all_values[0] - all_values[-1]) if len(all_values) > 1 else 0.0

        # Score: high Gini -> high inequality stress
        score = float(np.clip((gini_latest - 25) * 1.33, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "gini_latest": round(gini_latest, 2),
            "gini_latest_date": gini_date,
            "gini_mean": round(gini_mean, 2),
            "gini_trend": round(gini_trend, 2),
            "n_obs": len(rows),
            "method": "Gini proxy for Atkinson index; score = clip((Gini - 25) * 1.33, 0, 100)",
            "reference": "Atkinson 1970; Gini normalization per Sen 1973",
        }
