"""Systemic Risk Oversight.

Bank nonperforming loans ratio (FB.AST.NPER.ZS) measures the share of
bank loans that are in default or close to default, a key indicator of
systemic banking sector stress requiring regulatory intervention.

Score (0-100): clip(npl_ratio * 6.67, 0, 100).
NPL > 15% maps to CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class SystemicRiskOversight(LayerBase):
    layer_id = "lFR"
    name = "Systemic Risk Oversight"

    async def compute(self, db, **kwargs) -> dict:
        code = "FB.AST.NPER.ZS"
        name = "bank nonperforming loans"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no nonperforming loans data found",
            }

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no valid NPL values",
            }

        latest = values[0]
        score = float(max(0.0, min(100.0, latest * 6.67)))

        trend = None
        if len(values) >= 3:
            recent = values[:3]
            if recent[0] > recent[-1]:
                trend = "rising"
            elif recent[0] < recent[-1]:
                trend = "falling"
            else:
                trend = "stable"

        return {
            "score": round(score, 2),
            "npl_ratio_pct": round(latest, 2),
            "npl_trend": trend,
            "observations": len(values),
            "indicator": code,
            "interpretation": self._interpret(latest),
        }

    @staticmethod
    def _interpret(npl: float) -> str:
        if npl >= 15:
            return "critical NPL level: systemic banking crisis risk"
        if npl >= 10:
            return "high NPL: urgent regulatory intervention warranted"
        if npl >= 5:
            return "elevated NPL: enhanced oversight needed"
        return "NPL within acceptable range: banking sector relatively sound"
