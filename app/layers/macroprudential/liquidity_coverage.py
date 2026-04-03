"""Liquidity Coverage stress indicator.

Reserve adequacy as a proxy for banking sector liquidity stress.
Below 3 months import coverage signals potential liquidity pressure.

Score (0-100): max(0, 6 - reserves_months) * 12, clipped to 100.
0 months coverage = score 72 (CRISIS). 6+ months = score 0 (STABLE).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class LiquidityCoverage(LayerBase):
    layer_id = "lMP"
    name = "Liquidity Coverage"

    STRESS_THRESHOLD_MONTHS = 3.0
    ADEQUATE_THRESHOLD_MONTHS = 6.0

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 10)

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_code = 'FI.RES.TOTL.MO'
              AND ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
            ORDER BY dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no reserve adequacy data (FI.RES.TOTL.MO)",
            }

        values = [float(r["value"]) for r in rows]
        dates = [r["date"] for r in rows]
        latest = values[-1]

        score = float(np.clip(max(0.0, self.ADEQUATE_THRESHOLD_MONTHS - latest) * 12.0, 0.0, 100.0))

        trend = None
        if len(values) >= 3:
            delta = values[-1] - values[-3]
            trend = "improving" if delta > 0.3 else "deteriorating" if delta < -0.3 else "stable"

        return {
            "score": round(score, 2),
            "country": country,
            "reserves_import_months": round(latest, 2),
            "stress_threshold_months": self.STRESS_THRESHOLD_MONTHS,
            "adequate_threshold_months": self.ADEQUATE_THRESHOLD_MONTHS,
            "liquidity_stressed": latest < self.STRESS_THRESHOLD_MONTHS,
            "trend": trend,
            "date_range": {"start": dates[0], "end": dates[-1]},
            "observations": len(values),
            "interpretation": self._interpret(latest),
        }

    @staticmethod
    def _interpret(months: float) -> str:
        if months < 2.0:
            return f"critical liquidity stress: only {months:.1f} months import cover"
        if months < 3.0:
            return f"liquidity stress: {months:.1f} months below 3-month threshold"
        if months < 5.0:
            return f"adequate reserves: {months:.1f} months import cover"
        return f"comfortable reserves: {months:.1f} months import cover"
