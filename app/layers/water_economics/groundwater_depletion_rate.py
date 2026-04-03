"""Groundwater depletion rate: trend in freshwater withdrawal pressure.

Uses ER.H2O.FWTL.ZS (freshwater withdrawal % of internal resources) to
estimate the depletion trend via linear regression over available observations.
A rising trend signals accelerating groundwater stress.

Sources: World Bank WDI (ER.H2O.FWTL.ZS)
"""

from __future__ import annotations

from app.layers.base import LayerBase


class GroundwaterDepletionRate(LayerBase):
    layer_id = "lWA"
    name = "Groundwater Depletion Rate"

    async def compute(self, db, **kwargs) -> dict:
        code = "ER.H2O.FWTL.ZS"
        name = "freshwater withdrawals"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        vals = [row["value"] for row in rows if row["value"] is not None]
        if not vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No freshwater withdrawal data found",
            }

        latest = float(vals[0])

        # Linear regression on chronological order (vals are DESC, so reverse)
        chron_vals = [float(v) for v in reversed(vals)]
        n = len(chron_vals)

        trend = "stable"
        slope = 0.0
        r_squared = 0.0

        if n >= 3:
            from scipy import stats as sp_stats
            import numpy as np

            x = list(range(n))
            result = sp_stats.linregress(x, chron_vals)
            slope = float(result.slope)
            r_squared = float(result.rvalue ** 2)
            if result.pvalue < 0.10:
                trend = "increasing" if slope > 0 else "decreasing"

        # Score: baseline from current level, amplified by worsening trend
        base = min(latest / 100.0, 1.0) * 60.0
        trend_penalty = 0.0
        if trend == "increasing":
            trend_penalty = min(abs(slope) * 5.0, 30.0)
        elif trend == "decreasing":
            trend_penalty = -min(abs(slope) * 5.0, 20.0)

        score = round(min(100.0, max(0.0, base + trend_penalty)), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "withdrawal_pct_latest": round(latest, 2),
                "trend": trend,
                "trend_slope": round(slope, 4),
                "r_squared": round(r_squared, 3),
                "n_obs": n,
            },
        }
