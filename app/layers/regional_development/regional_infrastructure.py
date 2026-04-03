"""Regional Infrastructure module.

Proxies infrastructure distribution across regions using three national
access indicators as lower bounds on regional infrastructure provision.
Low national averages imply that lagging regions are even worse served.

Benchmarks: electricity access 100 %, paved roads 100 %, internet users 100 %
Gap score = (100 - indicator_value) for each indicator; averaged across the
three, then averaged over available years.

Score = clip(mean_gap, 0, 100)

Sources: WDI EG.ELC.ACCS.ZS (access to electricity % of population),
         WDI IS.ROD.PAVE.ZS (paved roads % of total roads),
         WDI IT.NET.USER.ZS (internet users % of population)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_SERIES = {
    "electricity": "EG.ELC.ACCS.ZS",
    "paved_roads": "IS.ROD.PAVE.ZS",
    "internet": "IT.NET.USER.ZS",
}


class RegionalInfrastructure(LayerBase):
    layer_id = "lRD"
    name = "Regional Infrastructure"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        latest_values = {}
        for label, series_id in _SERIES.items():
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT 5
                """,
                (country, series_id),
            )
            vals = [float(r["value"]) for r in rows if r["value"] is not None]
            if vals:
                latest_values[label] = {
                    "value": vals[0],
                    "date": rows[0]["date"],
                    "gap": max(0.0, 100.0 - vals[0]),
                }

        if not latest_values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        gaps = [v["gap"] for v in latest_values.values()]
        mean_gap = float(np.mean(gaps))
        score = float(np.clip(mean_gap, 0, 100))

        components = {
            label: {
                "value": round(v["value"], 2),
                "gap_from_100pct": round(v["gap"], 2),
                "date": v["date"],
            }
            for label, v in latest_values.items()
        }

        return {
            "score": round(score, 1),
            "country": country,
            "mean_infrastructure_gap": round(mean_gap, 2),
            "n_indicators": len(gaps),
            "components": components,
            "benchmark": "100% universal access for all three indicators",
            "series": _SERIES,
        }
