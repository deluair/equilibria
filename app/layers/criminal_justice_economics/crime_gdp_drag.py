"""Crime GDP drag: crime cost as % of GDP via homicide rate x per-capita cost proxy.

The economic cost of crime includes direct costs (victim losses, property damage,
medical treatment) and indirect costs (reduced investment, productivity loss, security
spending). Homicide rate serves as a reliable proxy for overall violent crime intensity.
Studies estimate per-homicide GDP costs at roughly 20-40x annual GDP per capita.

Score: very low homicide rate (<2/100k) -> STABLE, moderate (2-8) -> WATCH,
high (8-20) -> STRESS, very high (>20) -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class CrimeGDPDrag(LayerBase):
    layer_id = "lCJ"
    name = "Crime GDP Drag"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        hom_code = "VC.IHR.PSRC.P5"
        hom_name = "homicide"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (hom_code, f"%{hom_name}%"),
        )
        vals = [r["value"] for r in rows if r["value"] is not None]

        if not vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for homicide rate VC.IHR.PSRC.P5",
            }

        latest = vals[0]
        trend = round(vals[0] - vals[-1], 3) if len(vals) > 1 else None

        # Score maps homicide rate to crime GDP drag severity
        if latest < 2:
            score = 5.0 + latest * 5.0
        elif latest < 8:
            score = 15.0 + (latest - 2) * 4.5
        elif latest < 20:
            score = 42.0 + (latest - 8) * 2.5
        else:
            score = min(100.0, 72.0 + (latest - 20) * 1.4)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "homicide_rate_per_100k": round(latest, 2),
                "trend": trend,
                "n_obs": len(vals),
                "severity": (
                    "low" if latest < 2
                    else "moderate" if latest < 8
                    else "high" if latest < 20
                    else "very_high"
                ),
            },
        }
