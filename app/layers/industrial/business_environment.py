"""Business environment: ease of doing business rank as regulatory quality proxy.

The World Bank's Doing Business rank (IC.BUS.EASE.XQ) aggregates 10 regulatory
dimensions covering firm lifecycle from startup to closure: starting a business,
dealing with construction permits, getting electricity, registering property,
getting credit, protecting minority investors, paying taxes, trading across
borders, enforcing contracts, and resolving insolvency (World Bank 2020).

Rank 1 = best regulatory environment. Rank 190 = worst out of 190 economies.
A high rank number signals bureaucratic friction, legal uncertainty, and
regulatory opacity that suppress private investment, firm entry, and FDI.

Policy reform significance: a 10-rank improvement is associated with a
0.3-0.5 pp increase in GDP growth (Djankov et al. 2006).

Note: World Bank discontinued the Doing Business report in 2021 following
methodology concerns. The indicator remains in WDI for historical comparison.

Score formula (as specified):
    score = clip(rank / 190 * 100, 0, 100)
    Higher rank number = worse environment = higher stress score.

References:
    Djankov, S. et al. (2006). The effect of the business environment on
        firm performance. Journal of Financial Economics.
    World Bank (2020). Doing Business 2020. Washington DC.
    World Bank WDI: IC.BUS.EASE.XQ.

Indicator: IC.BUS.EASE.XQ (Ease of doing business score, 0-100 or rank 1-190).
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class BusinessEnvironment(LayerBase):
    layer_id = "l14"
    name = "Business Environment"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_id = 'IC.BUS.EASE.XQ'
              AND ds.country_iso3 = ?
              AND dp.value IS NOT NULL
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        if not rows or len(rows) < 1:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no ease of doing business data",
            }

        dates = [r["date"] for r in rows]
        values = np.array([float(r["value"]) for r in rows], dtype=float)

        latest = float(values[-1])

        # Determine if values look like ranks (1-190) or scores (0-100)
        # Ranks: score = rank / 190 * 100 (high rank = high stress)
        # Scores (0-100 scale): score = 100 - value (high score = good = low stress)
        if latest > 1.0:
            # Likely a rank
            rank = latest
            score = float(np.clip(rank / 190.0 * 100.0, 0.0, 100.0))
            metric_type = "rank"
        else:
            # Likely a normalized score 0-1
            rank = latest * 190.0
            score = float(np.clip(rank / 190.0 * 100.0, 0.0, 100.0))
            metric_type = "normalized"

        trend = None
        if len(values) >= 3:
            t = np.arange(len(values), dtype=float)
            slope, _, r_value, p_value, _ = linregress(t, values)
            trend = {
                "slope_per_year": round(float(slope), 4),
                "r_squared": round(float(r_value ** 2), 4),
                "p_value": round(float(p_value), 4),
                "direction": "improving" if slope < 0 else "deteriorating",
            }

        return {
            "score": round(score, 2),
            "country": country,
            "latest_rank": round(latest, 1),
            "latest_year": dates[-1],
            "metric_type": metric_type,
            "n_obs": len(values),
            "environment_tier": (
                "excellent" if latest <= 20
                else "good" if latest <= 60
                else "moderate" if latest <= 100
                else "poor" if latest <= 150
                else "very poor"
            ),
            "trend": trend,
        }
