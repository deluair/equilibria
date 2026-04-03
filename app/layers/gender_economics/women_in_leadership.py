"""Women in leadership module.

Measures female representation in parliament and managerial/senior positions.
Low representation signals structural barriers excluding women from economic
and political decision-making.

Score is based on the average gap from 50% parity across parliament and management:
    gap_parliament = max(0, 50 - female_parliament_pct)
    gap_management = max(0, 50 - female_managers_pct)
    avg_gap = (gap_parliament + gap_management) / 2
    score = clip(avg_gap * 2.5, 0, 100)

    avg_gap = 0%  -> score = 0   (parity)
    avg_gap = 10% -> score = 25  (watch)
    avg_gap = 20% -> score = 50  (stress)
    avg_gap = 30% -> score = 75
    avg_gap = 40% -> score = 100 (crisis: women hold ~10% of seats/positions)

Sources: WDI (SG.GEN.PARL.ZS parliament, SG.GEN.MNGT.ZS managers).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

SERIES_PARL = "SG.GEN.PARL.ZS"
SERIES_MGMT = "SG.GEN.MNGT.ZS"


class WomenInLeadership(LayerBase):
    layer_id = "lGE"
    name = "Women in Leadership"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('SG.GEN.PARL.ZS', 'SG.GEN.MNGT.ZS')
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no leadership representation data",
            }

        def latest(series_id: str) -> float | None:
            filtered = [r for r in rows if r["series_id"] == series_id]
            return float(filtered[0]["value"]) if filtered else None

        parl_pct = latest(SERIES_PARL)
        mgmt_pct = latest(SERIES_MGMT)

        if parl_pct is None and mgmt_pct is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "missing both parliament and management series",
            }

        gaps = []
        if parl_pct is not None:
            gaps.append(max(0.0, 50.0 - parl_pct))
        if mgmt_pct is not None:
            gaps.append(max(0.0, 50.0 - mgmt_pct))

        avg_gap = float(np.mean(gaps))
        score = float(np.clip(avg_gap * 2.5, 0.0, 100.0))
        latest_date = rows[0]["date"]

        return {
            "score": round(score, 2),
            "country": country,
            "female_parliament_pct": round(parl_pct, 2) if parl_pct is not None else None,
            "female_managers_pct": round(mgmt_pct, 2) if mgmt_pct is not None else None,
            "avg_gap_from_parity_pp": round(avg_gap, 2),
            "latest_date": latest_date,
            "note": "score = clip(avg_gap_from_50pct * 2.5, 0, 100). Series: SG.GEN.PARL.ZS + SG.GEN.MNGT.ZS",
        }
