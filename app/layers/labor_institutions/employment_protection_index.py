"""Employment protection legislation (EPL) composite score.

The OECD Employment Protection Index measures the stringency of regulations
governing individual dismissals, collective dismissals, and the use of
temporary contracts. It ranges from 0 (least restrictive) to 6 (most restrictive).

Very low EPL -> flexible but volatile labor markets; workers face high job
insecurity. Very high EPL -> protected insiders, excluded outsiders, reduced
hiring of marginal workers. An intermediate range (~2.0-2.5) is associated
with the best employment outcomes in the cross-country literature
(Nickell 1997, Blanchard & Wolfers 2000).

Scoring (stress peaks at both extremes; minimum near EPL = 2.25):
    deviation = |epl - 2.25|
    score = clip(deviation * 33, 0, 100)

    epl = 2.25  -> score ~ 0   (optimal)
    epl = 0     -> score = 74  (excessive flexibility)
    epl = 4.5   -> score = 74  (excessive rigidity)
    epl = 6.0   -> score = 100 (maximum rigidity)

Sources: OECD EPL database (eprc_v1 — overall EPL index)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

SERIES = "eprc_v1"
OPTIMAL_EPL = 2.25


class EmploymentProtectionIndex(LayerBase):
    layer_id = "lLI"
    name = "Employment Protection Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'eprc_v1'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no EPL data (eprc_v1)",
            }

        latest_date = rows[0]["date"]
        epl = float(rows[0]["value"])

        deviation = abs(epl - OPTIMAL_EPL)
        score = float(np.clip(deviation * 33.0, 0.0, 100.0))

        if epl < 1.0:
            regime = "very flexible"
        elif epl < 2.0:
            regime = "flexible"
        elif epl <= 2.5:
            regime = "balanced"
        elif epl <= 4.0:
            regime = "protective"
        else:
            regime = "very rigid"

        return {
            "score": round(score, 2),
            "country": country,
            "epl_index": round(epl, 3),
            "optimal_epl": OPTIMAL_EPL,
            "deviation_from_optimal": round(deviation, 3),
            "regime": regime,
            "latest_date": latest_date,
            "n_obs": len(rows),
            "note": (
                "score = clip(|epl - 2.25| * 33, 0, 100). "
                "Optimal EPL ~ 2.25. Series: eprc_v1 (0-6 scale)."
            ),
        }
