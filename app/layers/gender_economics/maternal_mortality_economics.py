"""Maternal mortality economics module.

Maternal mortality represents both a human rights failure and a human capital loss.
High maternal mortality rates (MMR) reduce female labor supply, increase household
poverty, and reflect inadequate investment in reproductive health infrastructure.

Scoring (log-scaled to handle wide country variation):
    MMR (per 100,000 live births):
    score = clip(log10(MMR + 1) / log10(1001) * 100, 0, 100)

    MMR = 0    -> score = 0   (no deaths)
    MMR = 9    -> score = 33  (approximately SDG target 70)
    MMR = 99   -> score = 67
    MMR = 999  -> score = 100 (crisis)

Sources: WDI (SH.STA.MMRT maternal mortality ratio per 100k live births).
"""

from __future__ import annotations

import math

import numpy as np

from app.layers.base import LayerBase

SERIES = "SH.STA.MMRT"
LOG_MAX = math.log10(1001.0)


class MaternalMortalityEconomics(LayerBase):
    layer_id = "lGE"
    name = "Maternal Mortality Economics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SH.STA.MMRT'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no maternal mortality data (SH.STA.MMRT)",
            }

        mmr = float(rows[0]["value"])
        if mmr < 0:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "invalid MMR value",
            }

        score = float(np.clip(math.log10(mmr + 1.0) / LOG_MAX * 100.0, 0.0, 100.0))

        # Trend over recent observations
        trend = "insufficient data"
        if len(rows) >= 3:
            vals = np.array([float(r["value"]) for r in sorted(rows[:10], key=lambda r: r["date"])], dtype=float)
            slope = float(np.polyfit(np.arange(len(vals), dtype=float), vals, 1)[0])
            trend = "rising" if slope > 1.0 else "falling" if slope < -1.0 else "stable"

        # SDG target comparison (70 per 100k by 2030)
        sdg_target = 70.0
        distance_to_sdg = round(mmr - sdg_target, 1)

        if mmr >= 500:
            severity = "crisis"
        elif mmr >= 100:
            severity = "stress"
        elif mmr >= 70:
            severity = "watch"
        else:
            severity = "stable"

        return {
            "score": round(score, 2),
            "country": country,
            "mmr_per_100k": round(mmr, 1),
            "sdg_target_per_100k": sdg_target,
            "distance_to_sdg_target": distance_to_sdg,
            "severity": severity,
            "trend": trend,
            "latest_date": rows[0]["date"],
            "n_obs": len(rows),
            "note": "score = clip(log10(MMR+1) / log10(1001) * 100, 0, 100). Series: SH.STA.MMRT",
        }
