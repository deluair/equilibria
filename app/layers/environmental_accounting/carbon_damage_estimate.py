"""Carbon damage estimate: NY.ADJ.DCO2.GN.ZS — CO2 damage as % of GNI.

The World Bank values CO2 damage using the social cost of carbon applied to
net CO2 emissions. This represents the global welfare cost of a country's
CO2 emissions, expressed relative to its gross national income.

Score: 0% -> 5, 5%+ -> 90.

References:
    World Bank WDI (NY.ADJ.DCO2.GN.ZS).
    Fankhauser, S. (1994). "The social costs of greenhouse gas emissions: an
        expected value approach." Energy Journal, 15(2), 157-184.
    Nordhaus, W. (2017). "Revisiting the social cost of carbon." PNAS, 114(7).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CarbonDamageEstimate(LayerBase):
    layer_id = "lEA"
    name = "Carbon Damage Estimate"

    async def compute(self, db, **kwargs) -> dict:
        code = "NY.ADJ.DCO2.GN.ZS"
        name = "CO2 damage"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no CO2 damage data"}

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid CO2 damage values"}

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
                "latest_co2_damage_pct_gni": round(latest, 2),
                "mean_co2_damage_pct_gni": round(avg, 2),
                "trend_slope": round(trend_slope, 4) if trend_slope is not None else None,
                "n_obs": len(values),
            },
        }
