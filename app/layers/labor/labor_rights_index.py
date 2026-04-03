"""Labor rights proxy via governance indicators.

Strong labor rights require an effective rule of law and voice/accountability
mechanisms (freedom of association, collective bargaining, labor inspection).
This module proxies labor rights using two World Governance Indicator series:
  - RL.EST: Rule of Law (-2.5 to +2.5)
  - VA.EST: Voice and Accountability (-2.5 to +2.5)

Low governance scores indicate weak institutional capacity to enforce labor
rights protections.

Scoring:
    combined = (rl + va) / 2   (range -2.5 to +2.5)
    score = clip(50 - combined * 20, 0, 100)

    combined = +2.5 -> score = 0  (strong governance, protected rights)
    combined = 0.0  -> score = 50
    combined = -2.5 -> score = 100 (governance failure, rights at risk)

Sources: WDI (WGI series RL.EST, VA.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

RL_SERIES = "RL.EST"
VA_SERIES = "VA.EST"


class LaborRightsIndex(LayerBase):
    layer_id = "l3"
    name = "Labor Rights Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('RL.EST', 'VA.EST')
              AND dp.value IS NOT NULL
            ORDER BY ds.series_id, dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no governance data (RL.EST, VA.EST)"}

        latest: dict[str, float] = {}
        latest_date: dict[str, str] = {}
        for r in rows:
            sid = r["series_id"]
            if sid not in latest:
                latest[sid] = float(r["value"])
                latest_date[sid] = r["date"]

        if RL_SERIES not in latest and VA_SERIES not in latest:
            return {"score": None, "signal": "UNAVAILABLE", "error": "both RL.EST and VA.EST missing"}

        # Use available series; neutral 0.0 if one is missing
        rl = latest.get(RL_SERIES, 0.0)
        va = latest.get(VA_SERIES, 0.0)
        combined = (rl + va) / 2.0

        score = float(np.clip(50.0 - combined * 20.0, 0.0, 100.0))

        # Classify
        if combined >= 1.0:
            rights_level = "strong"
        elif combined >= 0.0:
            rights_level = "moderate"
        elif combined >= -1.0:
            rights_level = "weak"
        else:
            rights_level = "very weak"

        result: dict = {
            "score": round(score, 2),
            "country": country,
            "governance_combined": round(combined, 4),
            "rights_level": rights_level,
            "n_obs": len(rows),
            "note": (
                "score = clip(50 - (rl + va)/2 * 20, 0, 100). "
                "WGI scale -2.5 to +2.5. Low = weak labor rights. Series: RL.EST, VA.EST."
            ),
        }

        if RL_SERIES in latest:
            result["rule_of_law_est"] = round(latest[RL_SERIES], 4)
            result["rule_of_law_date"] = latest_date.get(RL_SERIES)
        if VA_SERIES in latest:
            result["voice_accountability_est"] = round(latest[VA_SERIES], 4)
            result["voice_accountability_date"] = latest_date.get(VA_SERIES)

        return result
