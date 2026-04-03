"""Consumer Protection Index.

Combines Rule of Law (RL.EST) as a proxy for legal enforcement strength and
financial account ownership (FX.OWN.TOTL.ZS) as a proxy for financial access.
Strong legal enforcement + broad access = better consumer protection.

Score (0-100): higher protection = lower risk score.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class ConsumerProtectionIndex(LayerBase):
    layer_id = "lFR"
    name = "Consumer Protection Index"

    async def compute(self, db, **kwargs) -> dict:
        results = {}
        indicators = {
            "rule_of_law": ("RL.EST", "rule of law"),
            "account_ownership": ("FX.OWN.TOTL.ZS", "account ownership"),
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
                "error": "no consumer protection data found",
            }

        score_parts = []
        if "rule_of_law" in results:
            # RL.EST: -2.5 to +2.5. Higher = better enforcement = lower risk
            # Map +2.5 -> 0, -2.5 -> 100
            rl = results["rule_of_law"]
            score_parts.append(max(0.0, min(100.0, (-rl) * 20.0 + 50.0)))
        if "account_ownership" in results:
            # 0-100%. Higher ownership = better access = lower risk
            ao = results["account_ownership"]
            score_parts.append(max(0.0, min(100.0, 100.0 - ao)))

        score = float(sum(score_parts) / len(score_parts))

        return {
            "score": round(score, 2),
            "rule_of_law_est": results.get("rule_of_law"),
            "account_ownership_pct": results.get("account_ownership"),
            "indicators_found": len(results),
            "interpretation": self._interpret(score),
        }

    @staticmethod
    def _interpret(score: float) -> str:
        if score >= 75:
            return "weak consumer protection: poor legal enforcement and limited access"
        if score >= 50:
            return "below-average protection: gaps in enforcement or financial inclusion"
        if score >= 25:
            return "moderate consumer protection: improving but room for reform"
        return "strong consumer protection: robust legal framework and broad access"
