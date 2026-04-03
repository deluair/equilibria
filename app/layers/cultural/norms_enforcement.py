"""Norms Enforcement module.

Rule of law and regulatory quality composite from World Governance Indicators.

Queries RL.EST (Rule of Law) and RQ.EST (Regulatory Quality).
Low values indicate poor norms enforcement and weak regulatory capacity,
both of which generate economic stress through institutional uncertainty.

Scoring formula: score = clip(50 - mean(values) * 20, 0, 100)
- mean = +2.5 -> score = 0  (strong norms, no stress)
- mean = 0.0  -> score = 50
- mean = -2.5 -> score = 100 (severe norms failure)

Sources: WDI (WGI series RL.EST, RQ.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

SERIES = ("RL.EST", "RQ.EST")


class NormsEnforcement(LayerBase):
    layer_id = "lCU"
    name = "Norms Enforcement"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('RL.EST', 'RQ.EST')
            ORDER BY ds.series_id, dp.date DESC
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data (need >= 5 rows)",
            }

        latest: dict[str, float] = {}
        for r in rows:
            sid = r["series_id"]
            if sid not in latest:
                latest[sid] = float(r["value"])

        values = np.array(list(latest.values()), dtype=float)
        mean_val = float(np.mean(values))

        score = float(np.clip(50.0 - mean_val * 20.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "n_series": len(latest),
            "n_obs": len(rows),
            "norms_mean_wgi": round(mean_val, 4),
            "series_latest": {k: round(v, 4) for k, v in latest.items()},
            "note": "WGI scale -2.5 to +2.5; score = clip(50 - mean*20, 0, 100); high score = poor enforcement",
        }
