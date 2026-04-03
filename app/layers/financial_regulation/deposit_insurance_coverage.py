"""Deposit Insurance Coverage.

Uses private sector credit to GDP (FS.AST.PRVT.GD.ZS) as financial depth proxy
and account ownership (FX.OWN.TOTL.ZS) as coverage breadth proxy. Deeper
financial systems with broader account access require more robust deposit insurance.

Score (0-100): reflects adequacy gap between financial depth and coverage.
Higher financial depth with lower account ownership = higher risk.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class DepositInsuranceCoverage(LayerBase):
    layer_id = "lFR"
    name = "Deposit Insurance Coverage"

    async def compute(self, db, **kwargs) -> dict:
        results = {}
        indicators = {
            "private_credit_gdp": ("FS.AST.PRVT.GD.ZS", "private sector credit"),
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
                "error": "no deposit insurance coverage proxy data found",
            }

        score_parts = []
        if "private_credit_gdp" in results:
            # Higher credit depth = more exposure needing coverage
            # Normalize: 200% GDP = CRISIS (score 100)
            credit = results["private_credit_gdp"]
            score_parts.append(min(100.0, credit / 200.0 * 100.0))
        if "account_ownership" in results:
            # Higher ownership = more depositors to protect (coverage demand)
            # Invert: low ownership with high credit = worst gap
            ao = results["account_ownership"]
            if "private_credit_gdp" in results:
                # Gap metric: high credit / low access = high risk
                gap = max(0.0, results["private_credit_gdp"] - ao) / 100.0
                score_parts.append(min(100.0, gap * 100.0))
            else:
                score_parts.append(max(0.0, 100.0 - ao))

        score = float(sum(score_parts) / len(score_parts))

        return {
            "score": round(score, 2),
            "private_credit_pct_gdp": results.get("private_credit_gdp"),
            "account_ownership_pct": results.get("account_ownership"),
            "indicators_found": len(results),
            "interpretation": self._interpret(score),
        }

    @staticmethod
    def _interpret(score: float) -> str:
        if score >= 75:
            return "high deposit exposure: deposit insurance likely inadequate"
        if score >= 50:
            return "moderate exposure: insurance coverage may need strengthening"
        if score >= 25:
            return "manageable exposure: deposit insurance broadly adequate"
        return "low exposure: deposit insurance appears sufficient"
