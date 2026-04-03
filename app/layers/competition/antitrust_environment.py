"""Antitrust Environment module.

Proxies the strength of antitrust and competition policy using two
World Governance Indicators:
- Regulatory Quality (RQ.EST): capacity to formulate and implement
  sound policies and regulations that promote private sector development.
- Rule of Law (RL.EST): extent to which agents have confidence in and
  abide by the rules of society, including contract enforcement and courts.

Both range approximately -2.5 (worst) to +2.5 (best).

Weak antitrust = low RQ + low RL -> incumbents face little enforcement
risk and can maintain anti-competitive arrangements.

Score = clip(50 - (rq + rl) / 2 * 20, 0, 100).
  At (rq=rl=+2.5): score = 50 - 2.5*20 = 0 (strong antitrust, low stress).
  At (rq=rl=-2.5): score = 50 + 2.5*20 = 100 (weak antitrust, high stress).

Sources: World Governance Indicators (RQ.EST, RL.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class AntitrustEnvironment(LayerBase):
    layer_id = "lCO"
    name = "Antitrust Environment"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rq_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'RQ.EST'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        rl_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'RL.EST'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not rq_rows and not rl_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no governance data"}

        def latest_value(rows) -> float | None:
            for r in rows:
                if r["value"] is not None:
                    try:
                        return float(r["value"])
                    except (TypeError, ValueError):
                        pass
            return None

        rq = latest_value(rq_rows)
        rl = latest_value(rl_rows)

        # Fallback to 0 (neutral) if one is missing
        rq_val = rq if rq is not None else 0.0
        rl_val = rl if rl is not None else 0.0

        avg_governance = (rq_val + rl_val) / 2.0
        score = float(np.clip(50 - avg_governance * 20, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "regulatory_quality_est": round(rq_val, 3),
            "rule_of_law_est": round(rl_val, 3),
            "avg_governance": round(avg_governance, 3),
            "interpretation": (
                "strong antitrust environment" if score < 33
                else "moderate antitrust capacity" if score < 66
                else "weak antitrust / enforcement deficit"
            ),
            "reference": "Kaufmann et al. (2010): WGI; Motta (2004): competition policy",
        }
