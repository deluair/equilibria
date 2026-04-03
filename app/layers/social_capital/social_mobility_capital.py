"""Social Mobility Capital module.

Proxies intergenerational social mobility using:
  SE.PRM.CMPT.ZS  - Primary school completion rate (% of relevant age group)
  SI.POV.GINI     - Gini index (income inequality constraining mobility)

Higher completion rate = more opportunity = lower stress.
Higher Gini = less mobility = higher stress.

Score formula:
  completion_score = clip(100 - completion_pct, 0, 100)
  gini_score       = clip(gini, 0, 100)
  score = mean of available component scores

Sources: World Bank WDI (SE.PRM.CMPT.ZS, SI.POV.GINI)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SocialMobilityCapital(LayerBase):
    layer_id = "lSC"
    name = "Social Mobility Capital"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('SE.PRM.CMPT.ZS', 'SI.POV.GINI')
            ORDER BY ds.series_id, dp.date
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for SE.PRM.CMPT.ZS or SI.POV.GINI",
            }

        latest: dict[str, float] = {}
        series_values: dict[str, list[float]] = {}
        for r in rows:
            series_values.setdefault(r["series_id"], []).append(float(r["value"]))
        for sid, vals in series_values.items():
            latest[sid] = vals[-1]

        component_scores: list[float] = []
        completion_score = None
        gini_score = None

        if "SE.PRM.CMPT.ZS" in latest:
            completion_pct = latest["SE.PRM.CMPT.ZS"]
            completion_score = float(np.clip(100.0 - completion_pct, 0.0, 100.0))
            component_scores.append(completion_score)

        if "SI.POV.GINI" in latest:
            gini = latest["SI.POV.GINI"]
            gini_score = float(np.clip(gini, 0.0, 100.0))
            component_scores.append(gini_score)

        if not component_scores:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        score = float(np.mean(component_scores))

        return {
            "score": round(score, 1),
            "country": country,
            "primary_completion_pct": round(latest.get("SE.PRM.CMPT.ZS", float("nan")), 2),
            "gini_index": round(latest.get("SI.POV.GINI", float("nan")), 2),
            "completion_stress_score": round(completion_score, 1) if completion_score is not None else None,
            "gini_stress_score": round(gini_score, 1) if gini_score is not None else None,
            "n_components": len(component_scores),
            "note": "High score = constrained social mobility",
        }
