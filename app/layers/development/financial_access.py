"""Financial access: financial inclusion via bank account ownership.

Measures the share of adults with access to formal financial services.
Below 50% signals significant financial exclusion and development stress.

Key references:
    Demirguc-Kunt, A. et al. (2022). The Global Findex Database 2021.
        World Bank Group.
    Beck, T., Demirguc-Kunt, A. & Levine, R. (2007). Finance, inequality and
        the poor. Journal of Economic Growth, 12(1), 27-49.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

EXCLUSION_THRESHOLD = 50.0  # % account ownership below which exclusion is severe
SCORE_SCALE = 1.67           # score = max(0, 60 - ownership) * 1.67


class FinancialAccess(LayerBase):
    layer_id = "l4"
    name = "Financial Access"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Financial inclusion score from bank account ownership.

        Queries FX.OWN.TOTL.ZS (account ownership at a financial institution,
        % of population 15+). Score = clip(max(0, 60 - ownership_pct) * 1.67, 0, 100).
        Below 50% = exclusion stress.

        Returns dict with score, ownership %, exclusion flag, trend, and context.
        """
        country_iso3 = kwargs.get("country_iso3")

        rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'FX.OWN.TOTL.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not rows:
            return {"score": 50, "results": {"error": "no financial access data available"}}

        access_data: dict[str, dict[str, float]] = {}
        for r in rows:
            access_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        # Global distribution
        latest_vals = []
        for iso_data in access_data.values():
            if iso_data:
                yr = max(iso_data.keys())
                if iso_data[yr] is not None:
                    latest_vals.append(iso_data[yr])

        global_median = float(np.median(latest_vals)) if latest_vals else None
        excluded_countries = sum(1 for v in latest_vals if v < EXCLUSION_THRESHOLD)

        # Target country
        target_analysis = None
        score = 50.0

        if country_iso3 and country_iso3 in access_data:
            iso_data = access_data[country_iso3]
            years = sorted(iso_data.keys())
            if years:
                latest_pct = iso_data[years[-1]]
                raw_score = max(0.0, 60.0 - latest_pct) * SCORE_SCALE
                score = float(np.clip(raw_score, 0, 100))

                # Trend
                trend = None
                if len(years) >= 2:
                    change = latest_pct - iso_data[years[0]]
                    trend = "improving" if change > 5 else "stagnant" if change >= 0 else "declining"

                target_analysis = {
                    "latest_ownership_pct": latest_pct,
                    "exclusion_threshold": EXCLUSION_THRESHOLD,
                    "severely_excluded": latest_pct < EXCLUSION_THRESHOLD,
                    "global_median": global_median,
                    "below_global_median": latest_pct < global_median if global_median else None,
                    "trend": trend,
                    "n_observations": len(years),
                }
        elif latest_vals:
            global_gap = max(0.0, 60.0 - (global_median or 60.0))
            score = float(np.clip(global_gap * SCORE_SCALE, 0, 100))

        return {
            "score": score,
            "results": {
                "global_median_ownership_pct": global_median,
                "n_countries": len(access_data),
                "n_below_threshold": excluded_countries,
                "exclusion_threshold_pct": EXCLUSION_THRESHOLD,
                "target": target_analysis,
                "country_iso3": country_iso3,
            },
        }
