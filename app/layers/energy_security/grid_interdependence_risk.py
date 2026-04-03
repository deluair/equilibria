"""Grid interdependence risk: electricity transmission and distribution losses.

High electricity transmission and distribution losses indicate inefficient,
aging, or overloaded grid infrastructure that is vulnerable to cascading
failures and cross-border contagion in interconnected systems. WDI indicator
EG.ELC.LOSS.ZS measures electricity losses as a percentage of output.
High losses also signal poor grid resilience to supply shocks.

Score: low losses (<5%) -> STABLE efficient grid, moderate (5-15%) ->
WATCH aging infrastructure, high (15-25%) -> STRESS vulnerable,
very high (>25%) -> CRISIS fragile.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class GridInterdependenceRisk(LayerBase):
    layer_id = "lES"
    name = "Grid Interdependence Risk"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "EG.ELC.LOSS.ZS"
        name = "Electric power transmission and distribution losses"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for EG.ELC.LOSS.ZS"}

        vals = [r["value"] for r in rows if r["value"] is not None]
        if not vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all null values"}

        latest = vals[0]
        trend = round(vals[0] - vals[-1], 3) if len(vals) > 1 else None

        # Higher losses = weaker, more vulnerable grid
        if latest < 5:
            score = 5.0 + latest * 2.0
        elif latest < 15:
            score = 15.0 + (latest - 5) * 2.5
        elif latest < 25:
            score = 40.0 + (latest - 15) * 2.5
        else:
            score = min(100.0, 65.0 + (latest - 25) * 1.4)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "grid_losses_pct": round(latest, 2),
                "trend_pct_change": trend,
                "n_obs": len(vals),
                "grid_quality": (
                    "efficient" if latest < 5
                    else "moderate" if latest < 15
                    else "poor" if latest < 25
                    else "critical"
                ),
            },
        }
