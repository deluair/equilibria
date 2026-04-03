"""Part-time and temporary employment as precarious work indicator.

Involuntary part-time work reflects underemployment: workers accept part-time
hours because full-time jobs are unavailable. High involuntary part-time share
signals weak labor demand and earnings insecurity.

Primary series: SL.EMP.PART.ZS (part-time employment, % of total employment)
Fallback: if unavailable, proxy using wage employment vs total employment gap.
    wage_share = SL.EMP.WORK.ZS (wage and salaried workers, % total)
    proxy_rate = 100 - wage_share  (non-wage = likely part-time/casual)

Scoring:
    score = clip(part_time_rate * 2.0, 0, 100)

    rate = 0%  -> score = 0
    rate = 15% -> score = 30
    rate = 30% -> score = 60
    rate = 50% -> score = 100

Sources: WDI (SL.EMP.PART.ZS, fallback SL.EMP.WORK.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

PRIMARY_SERIES = "SL.EMP.PART.ZS"
FALLBACK_SERIES = "SL.EMP.WORK.ZS"


class PartTimeEmployment(LayerBase):
    layer_id = "l3"
    name = "Part-Time Employment"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('SL.EMP.PART.ZS', 'SL.EMP.WORK.ZS')
              AND dp.value IS NOT NULL
            ORDER BY ds.series_id, dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no part-time or wage employment data"}

        by_series: dict[str, list[tuple[str, float]]] = {}
        for r in rows:
            sid = r["series_id"]
            by_series.setdefault(sid, []).append((r["date"], float(r["value"])))

        # Prefer primary series
        used_series: str
        is_proxy = False

        if PRIMARY_SERIES in by_series:
            data = sorted(by_series[PRIMARY_SERIES], key=lambda x: x[0], reverse=True)
            rate = data[0][1]
            latest_date = data[0][0]
            used_series = PRIMARY_SERIES
        elif FALLBACK_SERIES in by_series:
            data = sorted(by_series[FALLBACK_SERIES], key=lambda x: x[0], reverse=True)
            wage_share = data[0][1]
            rate = max(0.0, 100.0 - wage_share)
            latest_date = data[0][0]
            used_series = FALLBACK_SERIES
            is_proxy = True
        else:
            return {"score": None, "signal": "UNAVAILABLE", "error": "neither SL.EMP.PART.ZS nor SL.EMP.WORK.ZS available"}

        score = float(np.clip(rate * 2.0, 0.0, 100.0))

        # Classify
        if rate >= 40:
            severity = "very high"
        elif rate >= 25:
            severity = "high"
        elif rate >= 10:
            severity = "moderate"
        else:
            severity = "low"

        return {
            "score": round(score, 2),
            "country": country,
            "part_time_rate_pct": round(rate, 2),
            "severity": severity,
            "latest_date": latest_date,
            "series_used": used_series,
            "is_proxy": is_proxy,
            "n_obs": len(rows),
            "note": (
                "score = clip(rate * 2.0, 0, 100). "
                "Primary: SL.EMP.PART.ZS. Fallback proxy: 100 - SL.EMP.WORK.ZS."
            ),
        }
