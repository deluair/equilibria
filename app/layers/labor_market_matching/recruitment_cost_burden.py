"""Recruitment cost burden: hiring cost as % of annual salary via WDI business environment.

High recruitment costs create barriers to labor market matching by discouraging
firms from posting vacancies and workers from switching jobs. The World Bank's
business environment indicators capture the regulatory and administrative cost
of hiring. Redundancy costs (weeks of salary) proxy for the full burden of
worker transitions including hiring and separation costs.

Score: low redundancy cost (<4 weeks) -> STABLE flexible market, moderate
(4-13 weeks) -> WATCH, high (13-26 weeks) -> STRESS rigid labor regulation,
very high (>26 weeks) -> CRISIS severely impeding job matching.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class RecruitmentCostBurden(LayerBase):
    layer_id = "lLM"
    name = "Recruitment Cost Burden"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        redund_code = "IC.REG.COST.PC.ZS"
        ease_code = "IC.BUS.EASE.XQ"

        redund_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (redund_code, "%redundancy cost%"),
        )
        ease_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (ease_code, "%ease of doing business%"),
        )

        redund_vals = [r["value"] for r in redund_rows if r["value"] is not None]
        ease_vals = [r["value"] for r in ease_rows if r["value"] is not None]

        if not redund_vals and not ease_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for recruitment cost indicators IC.REG.COST.PC.ZS / IC.BUS.EASE.XQ",
            }

        if redund_vals:
            cost = redund_vals[0]
            # Weeks of salary interpretation
            if cost < 4:
                score = cost * 4.0
            elif cost < 13:
                score = 16.0 + (cost - 4) * 3.0
            elif cost < 26:
                score = 43.0 + (cost - 13) * 2.0
            else:
                score = min(100.0, 69.0 + (cost - 26) * 1.0)
            cost_trend = round(redund_vals[0] - redund_vals[-1], 3) if len(redund_vals) > 1 else None
        else:
            # Use ease of doing business: higher rank (worse) -> higher cost burden
            cost = None
            ease = ease_vals[0]
            # Ease index 0-100 where higher = better; invert for cost burden
            burden = 100.0 - ease
            score = burden
            cost_trend = None

        # Adjust with ease of doing business if both available
        if redund_vals and ease_vals:
            ease = ease_vals[0]
            ease_penalty = max(0.0, (50.0 - ease) * 0.2)
            score = min(100.0, score + ease_penalty)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "redundancy_cost_weeks": round(redund_vals[0], 2) if redund_vals else None,
                "ease_of_business_index": round(ease_vals[0], 2) if ease_vals else None,
                "cost_trend": cost_trend,
                "n_obs_cost": len(redund_vals),
                "n_obs_ease": len(ease_vals),
                "market_flexibility": "flexible" if score < 25 else "rigid" if score > 50 else "moderate",
            },
        }
