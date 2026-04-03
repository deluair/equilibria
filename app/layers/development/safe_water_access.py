"""Safe water access: safely managed or basic drinking water access.

Measures population access to safely managed or basic drinking water.
Universal access (SDG 6.1) is the target. Gaps indicate a fundamental
development deficit with cascading health and productivity consequences.

Key references:
    WHO/UNICEF JMP (2023). Progress on household drinking water, sanitation
        and hygiene 2000-2022.
    UN Sustainable Development Goal 6.1: Achieve universal and equitable
        access to safe and affordable drinking water.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

TARGET_ACCESS = 95.0  # SDG aspirational threshold
SCORE_SCALE = 1.05     # score = max(0, 95 - access) * 1.05

# Prefer safely managed water; fall back to basic water service
SERIES_PREFERENCE = [
    "SH.H2O.SAFE.ZS",   # Safely managed drinking water (% population)
    "SH.H2O.BASW.ZS",   # Basic drinking water (% population)
]


class SafeWaterAccess(LayerBase):
    layer_id = "l4"
    name = "Safe Water Access"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Safe drinking water access gap score.

        Tries SH.H2O.SAFE.ZS first, then SH.H2O.BASW.ZS as fallback.
        Score = clip(max(0, 95 - access_pct) * 1.05, 0, 100).
        Below 95% = development deficit.

        Returns dict with score, access %, source series, gap, and crisis flag.
        """
        country_iso3 = kwargs.get("country_iso3")

        water_data: dict[str, dict[str, float]] = {}
        series_used: str | None = None

        for series_id in SERIES_PREFERENCE:
            rows = await db.fetch_all(
                """
                SELECT ds.country_iso3, dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.series_id = ?
                ORDER BY ds.country_iso3, dp.date
                """,
                [series_id],
            )
            if rows:
                for r in rows:
                    water_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]
                series_used = series_id
                break

        if not water_data:
            return {"score": 50, "results": {"error": "no water access data available"}}

        # Global distribution
        latest_vals = []
        for iso_data in water_data.values():
            if iso_data:
                yr = max(iso_data.keys())
                if iso_data[yr] is not None:
                    latest_vals.append(iso_data[yr])

        global_median = float(np.median(latest_vals)) if latest_vals else None
        below_target = sum(1 for v in latest_vals if v < TARGET_ACCESS)
        in_crisis = sum(1 for v in latest_vals if v < 50)

        # Target country
        target_analysis = None
        score = 50.0

        if country_iso3 and country_iso3 in water_data:
            iso_data = water_data[country_iso3]
            years = sorted(iso_data.keys())
            if years:
                latest_pct = iso_data[years[-1]]
                raw_score = max(0.0, TARGET_ACCESS - latest_pct) * SCORE_SCALE
                score = float(np.clip(raw_score, 0, 100))

                trend = None
                if len(years) >= 5:
                    old_pct = iso_data[years[-5]]
                    change = latest_pct - old_pct
                    trend = "improving" if change > 2 else "stagnant" if change >= 0 else "declining"

                target_analysis = {
                    "latest_access_pct": latest_pct,
                    "series_used": series_used,
                    "sdg_target_pct": TARGET_ACCESS,
                    "gap_from_target": max(0.0, TARGET_ACCESS - latest_pct),
                    "below_target": latest_pct < TARGET_ACCESS,
                    "water_crisis": latest_pct < 50,
                    "global_median": global_median,
                    "trend_5yr": trend,
                }
        elif latest_vals:
            avg_gap = float(np.mean([max(0.0, TARGET_ACCESS - v) for v in latest_vals]))
            score = float(np.clip(avg_gap * SCORE_SCALE, 0, 100))

        return {
            "score": score,
            "results": {
                "series_used": series_used,
                "sdg_target_pct": TARGET_ACCESS,
                "global_median_access_pct": global_median,
                "n_countries": len(water_data),
                "n_below_target": below_target,
                "n_in_crisis": in_crisis,
                "target": target_analysis,
                "country_iso3": country_iso3,
            },
        }
