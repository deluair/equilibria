"""Energy depletion rate: NY.ADJ.DNGY.GN.ZS — energy depletion as % of GNI.

Energy depletion covers the liquidation of subsoil fossil fuel assets (oil, gas, coal)
valued at resource rent = (market price - average extraction cost) * quantity extracted.
High depletion relative to GNI reflects dependence on finite fossil capital.

Score: 0% -> 5, 15%+ -> 90.

References:
    World Bank WDI (NY.ADJ.DNGY.GN.ZS).
    Hamilton, K. (1994). "Green adjustments to GDP." Resources Policy, 20(3), 155-168.
    World Bank (2021). "The Changing Wealth of Nations 2021."
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class EnergyDepletionRate(LayerBase):
    layer_id = "lEA"
    name = "Energy Depletion Rate"

    async def compute(self, db, **kwargs) -> dict:
        code = "NY.ADJ.DNGY.GN.ZS"
        name = "energy depletion"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no energy depletion data"}

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid energy depletion values"}

        latest = values[0]
        avg = float(np.mean(values))
        trend_slope = None
        if len(values) >= 3:
            xs = np.arange(len(values), dtype=float)
            trend_slope = float(np.polyfit(xs, values, 1)[0])

        # Score: 0% -> 5, 15% -> 90
        score = float(np.clip(5.0 + latest * 5.67, 5.0, 95.0))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "indicator_code": code,
                "latest_energy_depletion_pct_gni": round(latest, 2),
                "mean_energy_depletion_pct_gni": round(avg, 2),
                "trend_slope": round(trend_slope, 4) if trend_slope is not None else None,
                "n_obs": len(values),
            },
        }
