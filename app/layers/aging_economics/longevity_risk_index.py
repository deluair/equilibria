"""Longevity risk index: life expectancy vs retirement fund adequacy.

Longevity risk is the risk that individuals outlive their retirement savings.
As life expectancy increases, the required retirement capital grows, but
pension systems often fail to adjust contribution rates or retirement ages.
High life expectancy without adequate pension funding creates systemic
longevity risk at both individual and macroeconomic levels.

Score: very high life expectancy (>80) with inadequate systems -> STRESS,
moderate LE with adapted systems -> STABLE.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class LongevityRiskIndex(LayerBase):
    layer_id = "lAG"
    name = "Longevity Risk Index"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        le_code = "SP.DYN.LE00.IN"

        le_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (le_code, "%life expectancy%"),
        )

        if not le_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for life expectancy SP.DYN.LE00.IN",
            }

        le_vals = [r["value"] for r in le_rows if r["value"] is not None]
        if not le_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all null values for life expectancy"}

        latest_le = le_vals[0]
        # Trend over window
        le_trend = round(le_vals[0] - le_vals[-1], 2) if len(le_vals) > 1 else None

        # Longevity risk increases with life expectancy because pension systems
        # were typically designed for LE ~65-70. Standard retirement age 65.
        # Residual years post-retirement = LE - 65 (proxy for funding gap risk)
        residual_years = max(0.0, latest_le - 65.0)

        # Score: higher residual years = greater longevity risk
        # 0-5 yrs residual: STABLE, 5-15: WATCH, 15-20: STRESS, >20: CRISIS
        if residual_years < 5:
            score = 10.0 + residual_years * 2.0
        elif residual_years < 10:
            score = 20.0 + (residual_years - 5) * 4.0
        elif residual_years < 15:
            score = 40.0 + (residual_years - 10) * 5.0
        elif residual_years < 20:
            score = 65.0 + (residual_years - 15) * 4.0
        else:
            score = min(100.0, 85.0 + (residual_years - 20) * 1.5)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "life_expectancy_years": round(latest_le, 1),
                "residual_post_retirement_years": round(residual_years, 1),
                "le_trend_change": le_trend,
                "n_obs": len(le_vals),
                "longevity_risk_tier": (
                    "low" if residual_years < 5
                    else "moderate" if residual_years < 10
                    else "high" if residual_years < 15
                    else "very_high"
                ),
            },
        }
