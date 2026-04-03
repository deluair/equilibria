"""Capital Adequacy Regulation.

Bank capital to assets ratio (FB.BNK.CAPA.ZS) measures the soundness of
financial institutions relative to their risk-weighted assets.

Score (0-100): higher capital adequacy = lower risk.
clip((15 - ratio) * 5, 0, 100) — ratio < 3% maps to CRISIS, > 15% maps to STABLE.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class CapitalAdequacyRegulation(LayerBase):
    layer_id = "lFR"
    name = "Capital Adequacy Regulation"

    async def compute(self, db, **kwargs) -> dict:
        code = "FB.BNK.CAPA.ZS"
        name = "bank capital to assets"

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
                "error": "no capital adequacy data found",
            }

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no valid capital adequacy values",
            }

        latest = values[0]
        # Higher capital ratio = more resilient = lower risk score
        score = float(max(0.0, min(100.0, (15.0 - latest) * 5.0)))

        return {
            "score": round(score, 2),
            "capital_to_assets_ratio_pct": round(latest, 2),
            "observations": len(values),
            "indicator": code,
            "interpretation": self._interpret(latest),
        }

    @staticmethod
    def _interpret(ratio: float) -> str:
        if ratio >= 15:
            return "well-capitalized: strong regulatory buffer"
        if ratio >= 10:
            return "adequately capitalized: meets Basel minimums"
        if ratio >= 6:
            return "undercapitalized: regulatory concern"
        return "critically undercapitalized: systemic risk"
