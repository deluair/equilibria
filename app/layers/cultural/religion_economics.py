"""Religion Economics module.

Political stability and social cohesion as a proxy for societal
fragmentation with religious/ethnic dimensions.

Queries PV.EST (Political Stability and Absence of Violence/Terrorism)
from the World Governance Indicators. Low PV.EST scores indicate higher
political violence risk and social fragmentation, which are associated
with reduced long-run economic performance and institutional trust.

Scoring formula: score = clip(50 - pv_est * 20, 0, 100)
- PV.EST = +2.5 -> score = 0  (stable, cohesive)
- PV.EST = 0.0  -> score = 50
- PV.EST = -2.5 -> score = 100 (high fragmentation/violence)

Sources: WDI (WGI series PV.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

PV_SERIES = "PV.EST"


class ReligionEconomics(LayerBase):
    layer_id = "lCU"
    name = "Religion Economics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'PV.EST'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data (need >= 5 rows)",
            }

        values = np.array([float(r["value"]) for r in rows], dtype=float)
        latest_val = float(values[0])
        mean_val = float(np.mean(values))

        score = float(np.clip(50.0 - latest_val * 20.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(rows),
            "pv_est_latest": round(latest_val, 4),
            "pv_est_mean": round(mean_val, 4),
            "period": f"{rows[-1]['date']} to {rows[0]['date']}",
            "note": "PV.EST WGI -2.5 to +2.5; score = clip(50 - pv*20, 0, 100); high score = high fragmentation",
        }
