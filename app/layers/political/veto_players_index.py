"""Veto Players Index module.

Checks and balances proxy: governance composite dispersion across WGI dimensions.

Theory:
    Tsebelis (2002) defines veto players as actors whose agreement is required
    for policy change. Higher numbers of veto players increase policy stability
    but also gridlock risk. In countries with fragmented governance, different
    WGI dimensions (rule of law, government effectiveness, regulatory quality)
    diverge significantly -- the gap between formal institutions and actual
    effectiveness reflects institutional incoherence and fragmented veto
    structures. High cross-indicator dispersion signals institutional fragmentation.

Indicators:
    - RL.EST: Rule of Law (WGI). Range -2.5 to 2.5.
    - GE.EST: Government Effectiveness (WGI). Range -2.5 to 2.5.
    - RQ.EST: Regulatory Quality (WGI). Range -2.5 to 2.5.

Score construction:
    Compute latest values for each indicator. Dispersion = std across the three.
    score = clip(dispersion * 40, 0, 100)
    High dispersion = fragmented veto structure = higher institutional stress.

References:
    Tsebelis, G. (2002). Veto Players. Princeton UP.
    Henisz, W. (2002). "The Institutional Environment for Infrastructure Investment."
        Industrial and Corporate Change 11(2).
    World Bank. (2023). Worldwide Governance Indicators.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class VetoPlayersIndex(LayerBase):
    layer_id = "l12"
    name = "Veto Players Index"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate veto player fragmentation via governance dispersion.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 code (default BGD)
        """
        country = kwargs.get("country_iso3", "BGD")

        rl_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%RL.EST%' OR ds.name LIKE '%rule%of%law%estimate%'
                   OR ds.name LIKE '%rule%law%wgi%')
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        ge_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%GE.EST%' OR ds.name LIKE '%government%effectiveness%estimate%'
                   OR ds.name LIKE '%government%effectiveness%wgi%')
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        rq_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%RQ.EST%' OR ds.name LIKE '%regulatory%quality%estimate%'
                   OR ds.name LIKE '%regulatory%quality%wgi%')
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        available = [rows for rows in [rl_rows, ge_rows, rq_rows] if rows]
        if len(available) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient governance indicators for dispersion"}

        latest_vals = {}
        if rl_rows:
            latest_vals["rl"] = float(rl_rows[0]["value"])
        if ge_rows:
            latest_vals["ge"] = float(ge_rows[0]["value"])
        if rq_rows:
            latest_vals["rq"] = float(rq_rows[0]["value"])

        vals = list(latest_vals.values())
        dispersion = float(np.std(vals))
        score = float(np.clip(dispersion * 40, 0, 100))

        result = {
            "score": round(score, 2),
            "country": country,
            "governance_dispersion_std": round(dispersion, 4),
            "governance_latest": {k: round(v, 4) for k, v in latest_vals.items()},
            "fragmentation_level": (
                "high" if score > 50 else "moderate" if score > 25 else "low"
            ),
            "reference": "Tsebelis 2002; Henisz 2002; WGI RL.EST + GE.EST + RQ.EST",
        }

        return result
