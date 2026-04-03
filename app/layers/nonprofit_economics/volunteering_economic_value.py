"""Volunteering economic value: social capital and voluntary labor contribution.

Volunteering constitutes unpriced labor with significant economic value.
Time-use survey data is rarely available in international databases; proxied
via WDI social capital indicators including voice and accountability
(VA, Worldwide Governance Indicators) and civil liberties environment.
Higher civic participation and accountability correlate with higher
volunteering rates documented in cross-national social capital research.

Score: weak civil society environment -> CRISIS low volunteering; strong
voice and accountability -> STABLE high social capital and volunteer value.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class VolunteeringEconomicValue(LayerBase):
    layer_id = "lNP"
    name = "Volunteering Economic Value"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "IQ.CPA.PROP.XQ"
        name = "CPIA property rights"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        # Fallback: try governance/institutional quality indicator
        if not rows:
            fallback_code = "IQ.CPA.PUBS.XQ"
            fallback_name = "CPIA public sector management"
            rows = await db.fetch_all(
                "SELECT value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
                "ORDER BY date DESC LIMIT 15",
                (fallback_code, f"%{fallback_name}%"),
            )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for social capital/governance proxy",
            }

        values = [r["value"] for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all null values"}

        latest = values[0]
        trend = round(values[0] - values[-1], 3) if len(values) > 1 else None

        # CPIA scale is 1-6: higher = stronger institutions and civil society
        # Map to stress score: high CPIA -> low stress (inverted)
        if latest >= 4.5:
            score = 10.0 + (6.0 - latest) * 5.0
        elif latest >= 3.5:
            score = 17.5 + (4.5 - latest) * 22.5
        elif latest >= 2.5:
            score = 40.0 + (3.5 - latest) * 25.0
        else:
            score = min(100.0, 65.0 + (2.5 - latest) * 20.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "social_capital_index": round(latest, 3),
                "trend_change": trend,
                "n_obs": len(values),
                "civic_environment": (
                    "strong" if latest >= 4.5
                    else "moderate" if latest >= 3.5
                    else "weak" if latest >= 2.5
                    else "very_weak"
                ),
            },
        }
