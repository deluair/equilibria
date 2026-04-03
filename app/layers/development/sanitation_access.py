"""Sanitation access: basic sanitation development gap.

Measures population access to at least basic sanitation services.
The SDG 6.2 target is universal access. Below 95% signals a development
deficit; below 50% signals a severe sanitation crisis.

Key references:
    WHO/UNICEF JMP (2023). Progress on household drinking water, sanitation
        and hygiene 2000-2022.
    UN Sustainable Development Goal 6.2: Achieve access to adequate and
        equitable sanitation for all.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

TARGET_ACCESS = 95.0  # SDG aspirational threshold
SCORE_SCALE = 1.05     # score = max(0, 95 - access) * 1.05


class SanitationAccess(LayerBase):
    layer_id = "l4"
    name = "Sanitation Access"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Basic sanitation access gap score.

        Queries SH.STA.BASS.ZS (people using at least basic sanitation services,
        % of population). Score = clip(max(0, 95 - access_pct) * 1.05, 0, 100).
        Below 95% = development deficit.

        Returns dict with score, access %, gap, trend, and crisis flag.
        """
        country_iso3 = kwargs.get("country_iso3")

        rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.STA.BASS.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not rows:
            return {"score": 50, "results": {"error": "no sanitation access data available"}}

        sanitation_data: dict[str, dict[str, float]] = {}
        for r in rows:
            sanitation_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        # Global distribution
        latest_vals = []
        for iso_data in sanitation_data.values():
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

        if country_iso3 and country_iso3 in sanitation_data:
            iso_data = sanitation_data[country_iso3]
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
                    "sdg_target_pct": TARGET_ACCESS,
                    "gap_from_target": max(0.0, TARGET_ACCESS - latest_pct),
                    "below_target": latest_pct < TARGET_ACCESS,
                    "sanitation_crisis": latest_pct < 50,
                    "global_median": global_median,
                    "trend_5yr": trend,
                }
        elif latest_vals:
            avg_gap = float(np.mean([max(0.0, TARGET_ACCESS - v) for v in latest_vals]))
            score = float(np.clip(avg_gap * SCORE_SCALE, 0, 100))

        return {
            "score": score,
            "results": {
                "sdg_target_pct": TARGET_ACCESS,
                "global_median_access_pct": global_median,
                "n_countries": len(sanitation_data),
                "n_below_target": below_target,
                "n_in_crisis": in_crisis,
                "target": target_analysis,
                "country_iso3": country_iso3,
            },
        }
