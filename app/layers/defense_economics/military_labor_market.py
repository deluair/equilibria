"""Military labor market: military employment share of labor force.

The armed forces constitute a distinct segment of the labor market with
unique compensation, training, and career structures. A high military share
of the labor force indicates significant resource commitment to defense
employment and may reflect either security needs or economic absorption of
workers (particularly in high-unemployment developing economies).

WDI indicator MS.MIL.TOTL.TF.ZS captures armed forces personnel as % of
total labor force.

Score: very low (<0.3%) -> STABLE small professional force,
moderate (0.3-1%) -> WATCH, high (>2%) -> STRESS, very high (>4%) -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class MilitaryLaborMarket(LayerBase):
    layer_id = "lDX"
    name = "Military Labor Market"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "MS.MIL.TOTL.TF.ZS"
        name = "armed forces personnel"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        vals = [row["value"] for row in rows if row["value"] is not None]

        if not vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for armed forces personnel MS.MIL.TOTL.TF.ZS",
            }

        latest = vals[0]
        trend = round(vals[0] - vals[-1], 4) if len(vals) > 1 else None

        if latest < 0.3:
            score = 10.0
        elif latest < 1.0:
            score = 10.0 + (latest - 0.3) / 0.7 * 20.0
        elif latest < 2.0:
            score = 30.0 + (latest - 1.0) * 20.0
        elif latest < 4.0:
            score = 50.0 + (latest - 2.0) * 12.5
        else:
            score = min(100.0, 75.0 + (latest - 4.0) * 5.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "armed_forces_labor_share_pct": round(latest, 4),
                "trend_pct_change": trend,
                "n_obs": len(vals),
                "force_structure": (
                    "small professional" if latest < 0.3
                    else "moderate" if latest < 1.0
                    else "large" if latest < 2.0
                    else "mass mobilization" if latest < 4.0
                    else "extraordinary"
                ),
            },
        }
