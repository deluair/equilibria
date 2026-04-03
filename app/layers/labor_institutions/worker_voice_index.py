"""Worker voice index: worker representation and voice composite.

Worker voice encompasses formal and informal channels through which employees
can influence decisions affecting their working lives — works councils, board
representation, grievance mechanisms, and freedom of association. Strong voice
mechanisms are associated with higher productivity, lower turnover, and better
safety outcomes (Freeman & Medoff 1984; Rogers & Streeck 1995).

This module constructs a composite from two WGI sub-indicators that proxy
institutional capacity for worker voice:
  - VA.EST: Voice and Accountability (freedom of association, civil liberties)
  - RL.EST: Rule of Law (enforcement of labor rights)

Composite = (VA + RL) / 2, rescaled from [-2.5, +2.5] to [0, 100].
Higher score = stronger worker voice = lower institutional stress.

Scoring:
    normalized = (composite + 2.5) / 5.0   (0 to 1)
    score = clip((1 - normalized) * 100, 0, 100)

Sources: WDI (WGI series VA.EST, RL.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

VA_SERIES = "VA.EST"
RL_SERIES = "RL.EST"


class WorkerVoiceIndex(LayerBase):
    layer_id = "lLI"
    name = "Worker Voice Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('VA.EST', 'RL.EST')
              AND dp.value IS NOT NULL
            ORDER BY ds.series_id, dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no worker voice data (VA.EST, RL.EST)",
            }

        latest: dict[str, float] = {}
        latest_date: dict[str, str] = {}
        for r in rows:
            sid = r["series_id"]
            if sid not in latest:
                latest[sid] = float(r["value"])
                latest_date[sid] = r["date"]

        if VA_SERIES not in latest and RL_SERIES not in latest:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "both VA.EST and RL.EST missing",
            }

        va = latest.get(VA_SERIES, 0.0)
        rl = latest.get(RL_SERIES, 0.0)
        composite = (va + rl) / 2.0

        normalized = (composite + 2.5) / 5.0
        score = float(np.clip((1.0 - normalized) * 100.0, 0.0, 100.0))

        if composite >= 1.0:
            voice_level = "strong"
        elif composite >= 0.0:
            voice_level = "moderate"
        elif composite >= -1.0:
            voice_level = "weak"
        else:
            voice_level = "very weak"

        result: dict = {
            "score": round(score, 2),
            "country": country,
            "voice_composite": round(composite, 4),
            "voice_level": voice_level,
            "n_obs": len(rows),
            "note": (
                "score = clip((1 - (composite + 2.5) / 5) * 100, 0, 100). "
                "WGI scale -2.5 to +2.5. Series: VA.EST, RL.EST."
            ),
        }

        if VA_SERIES in latest:
            result["voice_accountability"] = round(latest[VA_SERIES], 4)
        if RL_SERIES in latest:
            result["rule_of_law"] = round(latest[RL_SERIES], 4)

        return result
