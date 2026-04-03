"""Mineral depletion rate: NY.ADJ.DMIN.GN.ZS — mineral depletion as % of GNI.

Captures the liquidation of subsoil mineral assets (metals, industrial minerals)
beyond replacement rates. Economies heavily dependent on mineral extraction without
saving or investing the rents face long-run capital erosion.

Score: 0% -> 5, 10%+ -> 90.

References:
    World Bank WDI (NY.ADJ.DMIN.GN.ZS).
    Auty, R. (1993). "Sustaining Development in Mineral Economies: The Resource
        Curse Thesis." Routledge.
    World Bank (2021). "The Changing Wealth of Nations 2021."
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MineralDepletionRate(LayerBase):
    layer_id = "lEA"
    name = "Mineral Depletion Rate"

    async def compute(self, db, **kwargs) -> dict:
        code = "NY.ADJ.DMIN.GN.ZS"
        name = "mineral depletion"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no mineral depletion data"}

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid mineral depletion values"}

        latest = values[0]
        avg = float(np.mean(values))
        trend_slope = None
        if len(values) >= 3:
            xs = np.arange(len(values), dtype=float)
            trend_slope = float(np.polyfit(xs, values, 1)[0])

        # Score: 0% -> 5, 10% -> 90
        score = float(np.clip(5.0 + latest * 8.5, 5.0, 95.0))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "indicator_code": code,
                "latest_mineral_depletion_pct_gni": round(latest, 2),
                "mean_mineral_depletion_pct_gni": round(avg, 2),
                "trend_slope": round(trend_slope, 4) if trend_slope is not None else None,
                "n_obs": len(values),
            },
        }
