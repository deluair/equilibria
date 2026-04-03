"""Renewable energy resilience: renewable share of total final energy consumption.

A higher renewable share reduces dependence on imported fossil fuels and
insulates the domestic energy system from global price volatility. WDI
EG.FEC.RNEW.ZS measures renewable energy as a percentage of total final
energy consumption, including modern renewables (solar, wind, hydro, geothermal)
and traditional biomass.

Score: high renewable share (>60%) -> STABLE resilient, moderate (40-60%) ->
WATCH transitioning, low (20-40%) -> STRESS fossil-dependent, very low (<20%)
-> CRISIS highly vulnerable.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class RenewableEnergyResilience(LayerBase):
    layer_id = "lES"
    name = "Renewable Energy Resilience"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "EG.FEC.RNEW.ZS"
        name = "Renewable energy consumption"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for EG.FEC.RNEW.ZS"}

        vals = [r["value"] for r in rows if r["value"] is not None]
        if not vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all null values"}

        latest = vals[0]
        trend = round(vals[0] - vals[-1], 3) if len(vals) > 1 else None

        # Higher renewable share = lower vulnerability = lower score
        if latest >= 60:
            score = max(0.0, 25.0 - (latest - 60) * 0.5)
        elif latest >= 40:
            score = 25.0 + (60.0 - latest) * 1.25
        elif latest >= 20:
            score = 50.0 + (40.0 - latest) * 1.25
        else:
            score = min(100.0, 75.0 + (20.0 - latest) * 1.25)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "renewable_share_pct": round(latest, 2),
                "trend_pct_change": trend,
                "n_obs": len(vals),
                "resilience_tier": (
                    "high" if latest >= 60
                    else "moderate" if latest >= 40
                    else "low" if latest >= 20
                    else "critical"
                ),
            },
        }
