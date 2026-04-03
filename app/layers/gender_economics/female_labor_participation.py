"""Female labor force participation module.

Measures the female labor force participation rate (FLFP) relative to the male
rate and a regional benchmark. A large gender gap in participation signals
exclusion of women from the formal economy.

FLFP gap = male_lfpr - female_lfpr (percentage points).
Benchmarks: gap < 5pp -> STABLE, gap > 40pp -> CRISIS.

Scoring:
    score = clip(gap_pp * 2.0, 0, 100)

    gap = 0pp  -> score = 0   (parity)
    gap = 12pp -> score = 25  (watch)
    gap = 25pp -> score = 50  (stress)
    gap = 38pp -> score = 75  (stress/crisis)
    gap = 50pp -> score = 100 (crisis)

Sources: WDI (SL.TLF.CACT.FE.ZS female, SL.TLF.CACT.MA.ZS male).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

SERIES_F = "SL.TLF.CACT.FE.ZS"
SERIES_M = "SL.TLF.CACT.MA.ZS"


class FemaleLabourParticipation(LayerBase):
    layer_id = "lGE"
    name = "Female Labour Force Participation"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('SL.TLF.CACT.FE.ZS', 'SL.TLF.CACT.MA.ZS')
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no LFPR data for female labour participation",
            }

        female_rows = [r for r in rows if r["series_id"] == SERIES_F]
        male_rows = [r for r in rows if r["series_id"] == SERIES_M]

        if not female_rows or not male_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "missing female or male LFPR series",
            }

        flfp = float(female_rows[0]["value"])
        mlfp = float(male_rows[0]["value"])
        gap_pp = mlfp - flfp
        score = float(np.clip(gap_pp * 2.0, 0.0, 100.0))

        # Trend over recent obs
        trend = "insufficient data"
        if len(female_rows) >= 3:
            vals = np.array([float(r["value"]) for r in sorted(female_rows[:10], key=lambda r: r["date"])], dtype=float)
            slope = float(np.polyfit(np.arange(len(vals), dtype=float), vals, 1)[0])
            trend = "rising" if slope > 0.2 else "falling" if slope < -0.2 else "stable"

        return {
            "score": round(score, 2),
            "country": country,
            "female_lfpr_pct": round(flfp, 2),
            "male_lfpr_pct": round(mlfp, 2),
            "participation_gap_pp": round(gap_pp, 2),
            "trend_female_lfpr": trend,
            "latest_date": female_rows[0]["date"],
            "note": "score = clip(gap_pp * 2.0, 0, 100). Series: SL.TLF.CACT.FE/MA.ZS",
        }
