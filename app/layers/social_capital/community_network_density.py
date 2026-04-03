"""Community Network Density module.

Uses internet penetration (IT.NET.USER.ZS, % of population) and mobile
subscriptions (IT.CEL.SETS.P2, per 100 people) as proxies for community
network density. Higher connectivity = stronger networks = lower stress.

Score formula:
  internet_score = clip(100 - internet_pct, 0, 100)
  mobile_score   = clip(100 - min(mobile_per100, 150) / 150 * 100, 0, 100)
  score = mean of available component scores

Sources: World Bank WDI (IT.NET.USER.ZS, IT.CEL.SETS.P2)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CommunityNetworkDensity(LayerBase):
    layer_id = "lSC"
    name = "Community Network Density"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('IT.NET.USER.ZS', 'IT.CEL.SETS.P2')
            ORDER BY ds.series_id, dp.date
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for IT.NET.USER.ZS or IT.CEL.SETS.P2",
            }

        latest: dict[str, float] = {}
        series_values: dict[str, list[float]] = {}
        for r in rows:
            series_values.setdefault(r["series_id"], []).append(float(r["value"]))
        for sid, vals in series_values.items():
            latest[sid] = vals[-1]

        component_scores: list[float] = []
        internet_score = None
        mobile_score = None

        if "IT.NET.USER.ZS" in latest:
            internet_pct = latest["IT.NET.USER.ZS"]
            internet_score = float(np.clip(100.0 - internet_pct, 0.0, 100.0))
            component_scores.append(internet_score)

        if "IT.CEL.SETS.P2" in latest:
            mobile_per100 = latest["IT.CEL.SETS.P2"]
            mobile_score = float(np.clip(100.0 - min(mobile_per100, 150.0) / 150.0 * 100.0, 0.0, 100.0))
            component_scores.append(mobile_score)

        if not component_scores:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        score = float(np.mean(component_scores))

        return {
            "score": round(score, 1),
            "country": country,
            "internet_users_pct": round(latest.get("IT.NET.USER.ZS", float("nan")), 2),
            "mobile_per_100": round(latest.get("IT.CEL.SETS.P2", float("nan")), 2),
            "internet_stress_score": round(internet_score, 1) if internet_score is not None else None,
            "mobile_stress_score": round(mobile_score, 1) if mobile_score is not None else None,
            "n_components": len(component_scores),
            "note": "High score = low connectivity density (less network capital)",
        }
