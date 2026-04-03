"""Banking Supervision Quality.

Combines World Bank Governance Indicators: Regulatory Quality (RQ.EST) and
Rule of Law (RL.EST). Both are in standard normal units (typically -2.5 to +2.5).

Score (0-100): maps composite governance percentile to risk.
Higher governance = lower regulatory risk = lower score.
score = clip((0 - composite) * 20 + 50, 0, 100).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class BankingSupervisionQuality(LayerBase):
    layer_id = "lFR"
    name = "Banking Supervision Quality"

    async def compute(self, db, **kwargs) -> dict:
        results = {}
        indicators = {
            "regulatory_quality": ("RQ.EST", "regulatory quality"),
            "rule_of_law": ("RL.EST", "rule of law"),
        }

        for key, (code, name) in indicators.items():
            rows = await db.fetch_all(
                "SELECT value FROM data_points WHERE series_id = ("
                "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
                "ORDER BY date DESC LIMIT 15",
                (code, f"%{name}%"),
            )
            if rows:
                vals = [float(r["value"]) for r in rows if r["value"] is not None]
                if vals:
                    results[key] = vals[0]

        if not results:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no governance indicator data found",
            }

        composite = sum(results.values()) / len(results)
        # Map: composite=-2.5 -> score=100 (CRISIS), composite=+2.5 -> score=0 (STABLE)
        score = float(max(0.0, min(100.0, (-composite) * 20.0 + 50.0)))

        return {
            "score": round(score, 2),
            "composite_governance": round(composite, 4),
            "regulatory_quality": round(results.get("regulatory_quality", float("nan")), 4),
            "rule_of_law": round(results.get("rule_of_law", float("nan")), 4),
            "indicators_found": len(results),
            "interpretation": self._interpret(composite),
        }

    @staticmethod
    def _interpret(composite: float) -> str:
        if composite >= 1.0:
            return "strong supervisory environment: low regulatory risk"
        if composite >= 0.0:
            return "moderate supervisory quality: adequate oversight"
        if composite >= -1.0:
            return "weak supervision: elevated regulatory gaps"
        return "poor governance: systemic supervisory failure risk"
