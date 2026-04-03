"""Systemic Risk Buffer (SRB) proxy.

Uses rapid credit expansion as a proxy for SIFI-related systemic risk buildup.
The 3-year average annual credit growth rate drives the score.

Score (0-100): clip(credit_growth_3yr_avg * 3, 0, 100).
Growth > 33% maps to CRISIS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SystemicRiskBuffer(LayerBase):
    layer_id = "lMP"
    name = "Systemic Risk Buffer"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 10)

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_code = 'FS.AST.DOMS.GD.ZS'
              AND ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
            ORDER BY dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if len(rows) < 4:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient domestic credit data for growth calculation",
            }

        values = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]

        # Annual growth rates (percentage point change, credit-to-GDP ratio)
        growth_rates = np.diff(values)

        # 3-year average of growth
        window = min(3, len(growth_rates))
        avg_growth = float(np.mean(growth_rates[-window:]))

        score = float(np.clip(avg_growth * 3.0, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "credit_to_gdp_latest_pct": round(float(values[-1]), 2),
            "credit_growth_3yr_avg_pp": round(avg_growth, 4),
            "growth_window_years": window,
            "date_range": {"start": dates[0], "end": dates[-1]},
            "interpretation": self._interpret(avg_growth),
        }

    @staticmethod
    def _interpret(growth: float) -> str:
        if growth > 25:
            return "rapid credit expansion: high systemic risk buildup"
        if growth > 15:
            return "elevated credit growth: systemic risk accumulating"
        if growth > 5:
            return "moderate credit growth: watchlist"
        if growth > 0:
            return "mild credit growth: low systemic pressure"
        return "credit contraction: deleveraging phase"
