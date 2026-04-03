"""Regulatory Compliance Cost.

Combines ease of doing business rank (IC.BUS.EASE.XQ) and days to register
a business (IC.REG.DURS). High compliance costs inhibit legitimate financial
activity and push actors toward unregulated channels.

Score (0-100): normalized composite. Higher cost = higher regulatory burden score.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class RegulatoryComplianceCost(LayerBase):
    layer_id = "lFR"
    name = "Regulatory Compliance Cost"

    async def compute(self, db, **kwargs) -> dict:
        results = {}
        indicators = {
            "ease_of_business": ("IC.BUS.EASE.XQ", "ease of doing business"),
            "days_to_register": ("IC.REG.DURS", "days to register"),
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
                "error": "no regulatory compliance cost data found",
            }

        # Ease of business rank: higher rank (worse) = higher score
        # Days to register: more days = higher score
        score_parts = []
        if "ease_of_business" in results:
            # Rank 1-190; normalize: rank/190 * 100
            score_parts.append(min(100.0, results["ease_of_business"] / 190.0 * 100.0))
        if "days_to_register" in results:
            # >100 days -> CRISIS; normalize days/100 * 100, clip at 100
            score_parts.append(min(100.0, results["days_to_register"] / 100.0 * 100.0))

        score = float(sum(score_parts) / len(score_parts))

        return {
            "score": round(score, 2),
            "ease_of_business_rank": results.get("ease_of_business"),
            "days_to_register": results.get("days_to_register"),
            "indicators_found": len(results),
            "interpretation": self._interpret(score),
        }

    @staticmethod
    def _interpret(score: float) -> str:
        if score >= 75:
            return "very high compliance burden: regulatory overreach or inefficiency"
        if score >= 50:
            return "elevated compliance cost: deters formal financial participation"
        if score >= 25:
            return "moderate compliance requirements: manageable"
        return "low compliance cost: efficient regulatory environment"
