"""Pollution damage cost: NY.ADJ.DPEM.GN.ZS — PM2.5 pollution damage as % of GNI.

PM2.5 ambient air pollution causes premature mortality and morbidity. The World Bank
estimates the economic cost via willingness-to-pay (VSL) methodology. Higher damage
as % of GNI indicates greater unaccounted environmental and health cost of pollution.

Score: 0% damage -> 5, 5%+ damage -> 90+.

References:
    World Bank WDI (NY.ADJ.DPEM.GN.ZS).
    Cropper, M. & Griffiths, C. (1994). "The interaction of population growth and
        environmental quality." AER, 84(2), 250-254.
    Ostro, B. (1994). "Estimating the health effects of air pollutants."
        World Bank Policy Research Working Paper 1301.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PollutionDamageCost(LayerBase):
    layer_id = "lEA"
    name = "Pollution Damage Cost"

    async def compute(self, db, **kwargs) -> dict:
        code = "NY.ADJ.DPEM.GN.ZS"
        name = "PM2.5 pollution damage"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no pollution damage data"}

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid pollution damage values"}

        latest = values[0]
        avg = float(np.mean(values))
        trend_slope = None
        if len(values) >= 3:
            xs = np.arange(len(values), dtype=float)
            trend_slope = float(np.polyfit(xs, values, 1)[0])

        # Score: 0% -> 5, 5% -> 90
        score = float(np.clip(5.0 + latest * 17.0, 5.0, 95.0))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "indicator_code": code,
                "latest_pm25_damage_pct_gni": round(latest, 2),
                "mean_pm25_damage_pct_gni": round(avg, 2),
                "trend_slope": round(trend_slope, 4) if trend_slope is not None else None,
                "n_obs": len(values),
            },
        }
