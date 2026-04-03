"""Judicial Independence module.

Estimates judicial independence using a composite of Rule of Law (RL.EST) and
Political Stability (PV.EST) as proxies. Rule of law captures adherence to
legal norms; political stability captures the absence of politically motivated
interference with judicial processes.

Composite = average(rl_latest, pv_latest) if both available, else whichever
is available.

Score formula:
  score = clip(50 - composite * 20, 0, 100)
  Low composite = weak judiciary = high stress.

Sources: World Bank WDI (RL.EST, PV.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class JudicialIndependence(LayerBase):
    layer_id = "lGV"
    name = "Judicial Independence"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('RL.EST', 'PV.EST')
            ORDER BY ds.series_id, dp.date
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        series: dict[str, list[float]] = {}
        series_dates: dict[str, list[str]] = {}
        for r in rows:
            sid = r["series_id"]
            series.setdefault(sid, []).append(float(r["value"]))
            series_dates.setdefault(sid, []).append(r["date"])

        latest: dict[str, float] = {k: v[-1] for k, v in series.items()}

        if not latest:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        composite = float(np.mean(list(latest.values())))
        score = float(np.clip(50.0 - composite * 20.0, 0.0, 100.0))

        all_dates = [d for dates in series_dates.values() for d in dates]

        return {
            "score": round(score, 1),
            "country": country,
            "composite": round(composite, 4),
            "rl_latest": round(latest["RL.EST"], 4) if "RL.EST" in latest else None,
            "pv_latest": round(latest["PV.EST"], 4) if "PV.EST" in latest else None,
            "indicators_used": list(latest.keys()),
            "period": f"{min(all_dates)} to {max(all_dates)}",
            "note": "Proxy: RL.EST + PV.EST average. Scale: -2.5 to +2.5",
        }
