"""Transparency Index module.

Measures government transparency as a composite of Control of Corruption
(CC.EST) and Voice and Accountability (VA.EST).

Transparency has two dimensions:
  1. Freedom from corruption (CC.EST): corrupt governments hide information,
     divert resources, and suppress accountability.
  2. Voice and accountability (VA.EST): free press, civil society, and open
     political processes are conduits for government transparency.

Composite = average(cc_latest, va_latest) if both available.
Score = clip(50 - composite * 20, 0, 100).
Low transparency = high stress.

Sources: World Bank WDI (CC.EST, VA.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TransparencyIndex(LayerBase):
    layer_id = "lGV"
    name = "Transparency Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('CC.EST', 'VA.EST')
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
            "cc_latest": round(latest["CC.EST"], 4) if "CC.EST" in latest else None,
            "va_latest": round(latest["VA.EST"], 4) if "VA.EST" in latest else None,
            "indicators_used": list(latest.keys()),
            "period": f"{min(all_dates)} to {max(all_dates)}",
            "interpretation": (
                "opaque governance" if score >= 65
                else "partial transparency" if score >= 40
                else "transparent governance"
            ),
            "note": "CC.EST + VA.EST average. Scale: -2.5 to +2.5",
        }
