"""Social enterprise viability: regulatory ease for mission-driven organizations.

Social enterprises operate at the intersection of market and civil society.
Their viability depends on the broader business regulatory environment proxied
by WDI business regulation indicators (time to start a business, cost of
starting a business, ease of doing business score). High regulatory burden
directly constrains the formation of hybrid social-mission organizations.

Score: low burden -> STABLE enabling environment, high burden -> CRISIS
inhospitable to social enterprise formation.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class SocialEnterpriseViability(LayerBase):
    layer_id = "lNP"
    name = "Social Enterprise Viability"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "IC.REG.DURS"
        name = "time required to start a business"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for IC.REG.DURS (business start time)",
            }

        values = [r["value"] for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all null values"}

        latest = values[0]
        trend = round(values[0] - values[-1], 3) if len(values) > 1 else None

        # Days to start a business: <5 -> very enabling, 5-15 -> moderate,
        # 15-30 -> constrained, >30 -> severe barrier
        if latest < 5:
            score = 10.0 + latest * 2.0
        elif latest < 15:
            score = 20.0 + (latest - 5) * 2.5
        elif latest < 30:
            score = 45.0 + (latest - 15) * 1.67
        else:
            score = min(100.0, 70.0 + (latest - 30) * 0.75)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "days_to_start_business": round(latest, 1),
                "trend_days_change": trend,
                "n_obs": len(values),
                "regulatory_burden": (
                    "very_low" if latest < 5
                    else "low" if latest < 15
                    else "moderate" if latest < 30
                    else "high"
                ),
            },
        }
