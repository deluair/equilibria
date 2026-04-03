"""Energy import dependence: energy imports as share of total energy use.

Countries importing a large fraction of their energy are exposed to supply
disruptions, price shocks, and geopolitical leverage from exporters. WDI
indicator EG.IMP.CONS.ZS measures net energy imports as a percentage of
total energy consumption (negative values indicate net exporters).

Score: net exporter or low import share (<20%) -> STABLE, moderate (20-50%)
-> WATCH, high (50-80%) -> STRESS, near-total (>80%) -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class EnergyImportDependence(LayerBase):
    layer_id = "lES"
    name = "Energy Import Dependence"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "EG.IMP.CONS.ZS"
        name = "Energy imports"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for EG.IMP.CONS.ZS"}

        vals = [r["value"] for r in rows if r["value"] is not None]
        if not vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all null values"}

        latest = vals[0]
        trend = round(vals[0] - vals[-1], 3) if len(vals) > 1 else None

        # Negative values = net exporter -> minimal risk
        if latest <= 0:
            score = max(0.0, 5.0 + latest * 0.1)
        elif latest < 20:
            score = 5.0 + latest * 0.95
        elif latest < 50:
            score = 24.0 + (latest - 20) * 0.87
        elif latest < 80:
            score = 50.0 + (latest - 50) * 0.83
        else:
            score = min(100.0, 75.0 + (latest - 80) * 1.25)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "energy_imports_pct_consumption": round(latest, 2),
                "trend_pct_change": trend,
                "n_obs": len(vals),
                "dependence_tier": (
                    "net_exporter" if latest <= 0
                    else "low" if latest < 20
                    else "moderate" if latest < 50
                    else "high" if latest < 80
                    else "critical"
                ),
            },
        }
