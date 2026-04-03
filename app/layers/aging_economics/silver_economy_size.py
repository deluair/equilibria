"""Silver economy size: elderly population share as economic opportunity.

The silver economy encompasses all economic activity serving the needs and
preferences of people aged 65+. A larger elderly share creates demand for
healthcare, assistive technology, financial services, leisure, and care
industries -- constituting a distinct and growing economic sector.

Score: low elderly share (<7%) -> STABLE opportunity-rich, moderate (7-14%) ->
WATCH maturing, high (>14%) -> STRESS advanced aging with structural shifts,
very high (>21%) -> CRISIS super-aged with fiscal pressure.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class SilverEconomySize(LayerBase):
    layer_id = "lAG"
    name = "Silver Economy Size"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "SP.POP.65UP.TO.ZS"
        name = "Population ages 65"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for SP.POP.65UP.TO.ZS"}

        values = [r["value"] for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all null values"}

        latest = values[0]
        # Trend: compare latest vs oldest in window
        trend = round(values[0] - values[-1], 3) if len(values) > 1 else None

        # Score: higher elderly share = more advanced silver economy (higher score = more stress)
        if latest < 7:
            score = 10.0 + latest * 2.0
        elif latest < 14:
            score = 24.0 + (latest - 7) * 3.0
        elif latest < 21:
            score = 45.0 + (latest - 14) * 3.5
        else:
            score = min(100.0, 69.5 + (latest - 21) * 2.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "elderly_share_pct": round(latest, 2),
                "trend_pct_change": trend,
                "n_obs": len(values),
                "aging_stage": (
                    "young" if latest < 7
                    else "maturing" if latest < 14
                    else "advanced" if latest < 21
                    else "super-aged"
                ),
            },
        }
