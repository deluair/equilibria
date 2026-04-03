"""Palma Ratio module.

Approximates the Palma ratio (top 10% share / bottom 40% share) from the
Gini coefficient using the Palma-Gini approximation formula.

Formula (Cobham & Sumner 2013 approximation):
    palma ≈ (1 + Gini/100) / (1 - Gini/100) * 0.9

The Palma ratio focuses on the extremes of the distribution, which Gini
underweights relative to the middle.

Score = clip((palma - 1) * 30, 0, 100)
  - palma = 1.0 is borderline (all segments equal)
  - palma = 4.3+ maps to score ~ 100 (extreme top concentration)

Sources: SI.POV.GINI (World Bank WDI)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PalmaRatio(LayerBase):
    layer_id = "lIQ"
    name = "Palma Ratio"

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
            LIMIT 5
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        gini_raw = float(rows[0]["value"])
        date = rows[0]["date"]

        # Gini on 0-100 scale; normalize to 0-1 for formula
        g = gini_raw / 100.0
        g = float(np.clip(g, 0.01, 0.99))

        # Palma-Gini approximation
        palma = (1.0 + g) / (1.0 - g) * 0.9

        score = float(np.clip((palma - 1.0) * 30.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "date": date,
            "gini": round(gini_raw, 2),
            "palma_ratio_approx": round(palma, 3),
            "interpretation": {
                "palma_gt_2": palma > 2.0,
                "palma_gt_3": palma > 3.0,
                "note": "Palma > 2 means top 10% earn > 2x what bottom 40% earn combined",
            },
        }
