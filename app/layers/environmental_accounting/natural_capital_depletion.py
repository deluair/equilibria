"""Natural capital depletion: NY.ADJ.DRES.GN.ZS — natural resource depletion as % of GNI.

Captures the aggregate depletion of natural resources (energy, minerals, forests)
relative to gross national income. Higher depletion rates reduce long-run wealth
and signal reliance on non-renewable resource drawdown to sustain consumption.

Score: depletion % of GNI mapped to 0-100 stress (higher depletion = higher stress).

References:
    World Bank WDI (NY.ADJ.DRES.GN.ZS).
    World Bank (2021). "The Changing Wealth of Nations 2021."
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class NaturalCapitalDepletion(LayerBase):
    layer_id = "lEA"
    name = "Natural Capital Depletion"

    async def compute(self, db, **kwargs) -> dict:
        code = "NY.ADJ.DRES.GN.ZS"
        name = "natural resource depletion"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no natural capital depletion data"}

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid depletion values"}

        latest = values[0]
        avg = float(np.mean(values))
        trend_slope = None
        if len(values) >= 3:
            xs = np.arange(len(values), dtype=float)
            trend_slope = float(np.polyfit(xs, values, 1)[0])

        # Score: 0% depletion -> 5, 20%+ depletion -> 95
        score = float(np.clip(5.0 + latest * 4.5, 5.0, 95.0))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "indicator_code": code,
                "latest_depletion_pct_gni": round(latest, 2),
                "mean_depletion_pct_gni": round(avg, 2),
                "trend_slope": round(trend_slope, 4) if trend_slope is not None else None,
                "n_obs": len(values),
            },
        }
