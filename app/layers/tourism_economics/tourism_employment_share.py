"""Tourism Employment Share module.

Proxies tourism's contribution to employment via services employment as a
share of total employment (SL.SRV.EMPL.ZS — World Bank WDI). In most
economies, tourism is embedded within the services sector; a growing services
employment share signals capacity to absorb tourism-driven labour demand.

Score: 0 (high services employment, good absorption) to 100 (low services,
poor labour market alignment with tourism).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TourismEmploymentShare(LayerBase):
    layer_id = "lTO"
    name = "Tourism Employment Share"

    async def compute(self, db, **kwargs) -> dict:
        code = "SL.SRV.EMPL.ZS"
        name = "services employment"

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
                "error": "no data for SL.SRV.EMPL.ZS (services employment % total)",
            }

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "all values null for services employment share",
            }

        latest = values[0]
        avg = float(np.mean(values))

        # Trend
        trend_slope = 0.0
        if len(values) >= 3:
            arr = np.array(values[::-1])
            t = np.arange(len(arr), dtype=float)
            slope, _ = np.polyfit(t, arr, 1)
            trend_slope = float(slope)

        # Higher services employment = better tourism labour absorption = lower score
        # Map: 10% -> 85, 40% -> 50, 70% -> 20, 90% -> 5
        base_score = float(np.clip(90 - latest * 0.95, 5, 95))
        # Declining trend adds penalty
        trend_penalty = float(np.clip(-trend_slope * 3, 0, 10))
        score = float(np.clip(base_score + trend_penalty, 0, 100))

        return {
            "score": round(score, 1),
            "indicator": code,
            "services_employment_pct": round(latest, 2),
            "mean_pct": round(avg, 2),
            "trend_slope_per_period": round(trend_slope, 4),
            "n_obs": len(values),
            "methodology": "score = clip(90 - share * 0.95 + trend_penalty, 0, 100)",
        }
