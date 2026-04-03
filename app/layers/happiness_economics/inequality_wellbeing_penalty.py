"""Inequality wellbeing penalty: Gini coefficient impact on life satisfaction.

Extensive cross-national research (Wilkinson & Pickett, Alesina et al.)
documents that higher income inequality -- measured by the Gini coefficient --
reduces average life satisfaction even after controlling for mean income.
The mechanism includes: relative deprivation, reduced social trust, weaker
public goods provision, and higher perceived status anxiety.

A Gini above 40 signals significant wellbeing penalty; above 50 (Latin America
style) signals severe societal stress. Countries below 30 (Nordic) show the
lowest wellbeing penalty from inequality.

Score: Gini < 30 -> STABLE, 30-40 -> WATCH, 40-50 -> STRESS, >50 -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class InequalityWellbeingPenalty(LayerBase):
    layer_id = "lHE"
    name = "Inequality Wellbeing Penalty"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        gini_code = "SI.POV.GINI"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gini_code, "%Gini%"),
        )
        vals = [r["value"] for r in rows if r["value"] is not None]

        if not vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for SI.POV.GINI",
            }

        latest = vals[0]
        trend = round(vals[0] - vals[-1], 3) if len(vals) > 1 else None

        # Score mapped from Gini thresholds
        if latest < 25:
            score = 5.0 + latest * 0.4
        elif latest < 30:
            score = 15.0 + (latest - 25) * 1.6
        elif latest < 40:
            score = 23.0 + (latest - 30) * 2.7
        elif latest < 50:
            score = 50.0 + (latest - 40) * 2.5
        else:
            score = min(100.0, 75.0 + (latest - 50) * 1.5)

        # Rising inequality increases score slightly
        if trend is not None and trend > 0:
            score = min(100.0, score + trend * 1.5)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "gini_index": round(latest, 2),
                "gini_trend": trend,
                "inequality_tier": (
                    "low"
                    if latest < 30
                    else "moderate"
                    if latest < 40
                    else "high"
                    if latest < 50
                    else "extreme"
                ),
                "n_obs": len(vals),
            },
        }
