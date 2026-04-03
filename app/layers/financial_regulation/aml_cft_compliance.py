"""AML/CFT Compliance.

Anti-Money Laundering and Countering the Financing of Terrorism compliance
is proxied by Rule of Law (RL.EST) and Control of Corruption (CC.EST).
Weak governance on both dimensions signals elevated ML/FT risk.

Score (0-100): composite governance deficit. Higher = greater AML/CFT risk.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class AmlCftCompliance(LayerBase):
    layer_id = "lFR"
    name = "AML/CFT Compliance"

    async def compute(self, db, **kwargs) -> dict:
        results = {}
        indicators = {
            "rule_of_law": ("RL.EST", "rule of law"),
            "control_of_corruption": ("CC.EST", "control of corruption"),
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
                "error": "no AML/CFT governance data found",
            }

        composite = sum(results.values()) / len(results)
        # Map: composite=-2.5 -> score=100, composite=+2.5 -> score=0
        score = float(max(0.0, min(100.0, (-composite) * 20.0 + 50.0)))

        return {
            "score": round(score, 2),
            "composite_governance": round(composite, 4),
            "rule_of_law_est": results.get("rule_of_law"),
            "control_of_corruption_est": results.get("control_of_corruption"),
            "indicators_found": len(results),
            "interpretation": self._interpret(composite),
        }

    @staticmethod
    def _interpret(composite: float) -> str:
        if composite >= 1.0:
            return "strong AML/CFT framework: low illicit finance risk"
        if composite >= 0.0:
            return "adequate framework: moderate compliance posture"
        if composite >= -1.0:
            return "weak framework: elevated AML/CFT vulnerabilities"
        return "poor governance: high ML/FT risk, likely FATF grey/black list risk"
