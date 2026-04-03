"""Indigenous Economics module.

Property rights and legal system quality as a proxy for indigenous
and minority economic rights. Weak legal rights and property frameworks
deter investment and constrain economic participation for marginalised groups.

Primary query: IC.LGL.CRED.XQ (Strength of legal rights index, 0-12).
Fallback: RL.EST (Rule of Law WGI, -2.5 to +2.5).

Scoring:
  Primary (legal rights index 0-12):
    score = clip((12 - index) / 12 * 100, 0, 100)
    - index = 12 -> score = 0  (strong rights)
    - index = 6  -> score = 50
    - index = 0  -> score = 100

  Fallback (RL.EST):
    score = clip(50 - rl_est * 20, 0, 100)

Sources: WDI (IC.LGL.CRED.XQ, fallback RL.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

PRIMARY_SERIES = "IC.LGL.CRED.XQ"
FALLBACK_SERIES = "RL.EST"
MAX_INDEX = 12.0


class IndigenousEconomics(LayerBase):
    layer_id = "lCU"
    name = "Indigenous Economics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('IC.LGL.CRED.XQ', 'RL.EST')
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

        primary_rows = [r for r in rows if r["series_id"] == PRIMARY_SERIES]
        fallback_rows = [r for r in rows if r["series_id"] == FALLBACK_SERIES]

        if primary_rows:
            values = np.array([float(r["value"]) for r in primary_rows], dtype=float)
            latest_val = float(values[0])
            score = float(np.clip((MAX_INDEX - latest_val) / MAX_INDEX * 100.0, 0.0, 100.0))
            method = PRIMARY_SERIES
            metric_val = round(latest_val, 4)
        elif fallback_rows:
            values = np.array([float(r["value"]) for r in fallback_rows], dtype=float)
            latest_val = float(values[0])
            score = float(np.clip(50.0 - latest_val * 20.0, 0.0, 100.0))
            method = FALLBACK_SERIES
            metric_val = round(latest_val, 4)
        else:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no usable series found",
            }

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(rows),
            "method": method,
            "metric_value": metric_val,
            "note": (
                "primary: score = clip((12 - index)/12*100, 0, 100); "
                "fallback RL.EST: score = clip(50 - rl*20, 0, 100)"
            ),
        }
