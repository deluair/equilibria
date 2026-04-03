"""Forest depletion rate: NY.ADJ.DFOR.GN.ZS — net forest depletion as % of GNI.

Net forest depletion = value of timber harvest beyond sustainable yield + carbon
value of standing forests lost to deforestation. Captures the liquidation of
forest natural capital that is excluded from conventional GDP.

Score: 0% -> 5, 3%+ -> 90.

References:
    World Bank WDI (NY.ADJ.DFOR.GN.ZS).
    Lange, G. et al. (2018). "The Changing Wealth of Nations 2018."
        World Bank Group, Washington DC.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ForestDepletionRate(LayerBase):
    layer_id = "lEA"
    name = "Forest Depletion Rate"

    async def compute(self, db, **kwargs) -> dict:
        code = "NY.ADJ.DFOR.GN.ZS"
        name = "net forest depletion"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no forest depletion data"}

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid forest depletion values"}

        latest = values[0]
        avg = float(np.mean(values))
        trend_slope = None
        if len(values) >= 3:
            xs = np.arange(len(values), dtype=float)
            trend_slope = float(np.polyfit(xs, values, 1)[0])

        # Score: 0% -> 5, 3% -> 90
        score = float(np.clip(5.0 + latest * 28.3, 5.0, 95.0))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "indicator_code": code,
                "latest_forest_depletion_pct_gni": round(latest, 2),
                "mean_forest_depletion_pct_gni": round(avg, 2),
                "trend_slope": round(trend_slope, 4) if trend_slope is not None else None,
                "n_obs": len(values),
            },
        }
