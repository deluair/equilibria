"""Social Cohesion Score module.

Composite of income inequality (GINI, SI.POV.GINI) and political stability
(PV.EST) from World Bank WDI/WGI.

Score formula:
  gini_score = clip(gini / 100 * 100, 0, 100)   [higher Gini = more stress]
  stability_score = clip(50 - pv_est * 20, 0, 100) [higher PV = less stress]
  cohesion_score = mean(gini_score, stability_score)

Sources: World Bank WDI (SI.POV.GINI, PV.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SocialCohesionScore(LayerBase):
    layer_id = "lSC"
    name = "Social Cohesion Score"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('SI.POV.GINI', 'PV.EST')
            ORDER BY ds.series_id, dp.date
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for SI.POV.GINI or PV.EST",
            }

        latest: dict[str, float] = {}
        series_values: dict[str, list[float]] = {}
        for r in rows:
            series_values.setdefault(r["series_id"], []).append(float(r["value"]))
        for sid, vals in series_values.items():
            latest[sid] = vals[-1]

        component_scores: list[float] = []
        gini_score = None
        stability_score = None

        if "SI.POV.GINI" in latest:
            gini = latest["SI.POV.GINI"]
            gini_score = float(np.clip(gini, 0.0, 100.0))
            component_scores.append(gini_score)

        if "PV.EST" in latest:
            pv = latest["PV.EST"]
            stability_score = float(np.clip(50.0 - pv * 20.0, 0.0, 100.0))
            component_scores.append(stability_score)

        if not component_scores:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        score = float(np.mean(component_scores))

        return {
            "score": round(score, 1),
            "country": country,
            "gini_index": round(latest.get("SI.POV.GINI", float("nan")), 2),
            "gini_stress_score": round(gini_score, 1) if gini_score is not None else None,
            "political_stability_est": round(latest.get("PV.EST", float("nan")), 4),
            "stability_stress_score": round(stability_score, 1) if stability_score is not None else None,
            "n_components": len(component_scores),
            "note": "Higher score = lower cohesion (more stress)",
        }
