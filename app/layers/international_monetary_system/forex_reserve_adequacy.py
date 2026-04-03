"""Forex reserve adequacy: months of import coverage from FX reserves.

The IMF's reserve adequacy framework evaluates whether foreign exchange
reserves are sufficient to cover short-term external obligations. The
traditional metric is months of imports covered by reserves: the rule
of thumb is 3 months as the minimum adequate level.

WDI FI.RES.TOTL.MO provides total reserves in months of imports directly.

Score: >6 months -> STABLE (strong buffer), 3-6 months -> WATCH (adequate
but thin), 1.5-3 months -> STRESS (below minimum), <1.5 months -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class ForexReserveAdequacy(LayerBase):
    layer_id = "lMS"
    name = "Forex Reserve Adequacy"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "FI.RES.TOTL.MO"
        name = "Total reserves in months of imports"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        vals = [r["value"] for r in rows if r["value"] is not None]
        if not vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for FI.RES.TOTL.MO",
            }

        latest = vals[0]
        trend = round(vals[0] - vals[-1], 3) if len(vals) > 1 else None

        # Score: lower months = more stress; IMF 3-month rule
        if latest >= 6:
            score = 8.0
        elif latest >= 4:
            score = 8.0 + (6 - latest) * 7.0
        elif latest >= 3:
            score = 22.0 + (4 - latest) * 13.0
        elif latest >= 1.5:
            score = 35.0 + (3 - latest) * 20.0
        else:
            score = min(100.0, 65.0 + (1.5 - latest) * 23.3)

        # Declining trend adds pressure
        if trend is not None and trend < -1:
            score = min(100.0, score + 8.0)

        score = round(score, 2)
        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "reserves_months_imports": round(latest, 2),
                "trend_change": trend,
                "n_obs": len(vals),
                "imf_3month_rule_met": latest >= 3,
                "adequacy_category": (
                    "strong" if latest >= 6
                    else "adequate" if latest >= 3
                    else "below_minimum" if latest >= 1.5
                    else "critical"
                ),
            },
        }
