"""Green GDP gap: difference between GDP growth and environmentally-adjusted GDP growth.

Measures how much of recorded GDP growth disappears once environmental costs
(CO2 damage + resource depletion) are deducted. A large positive gap means
GDP growth overstates true welfare improvement.

Green GDP growth = GDP growth - d(environmental cost % GNI)
Gap = GDP growth - Green GDP growth = change in environmental cost share

Score: larger gap (more overstated growth) -> higher stress.

References:
    Lawn, P. (2003). "A theoretical foundation to support the Index of Sustainable
        Economic Welfare." Ecological Economics, 44(1), 105-118.
    World Bank WDI: NY.ADJ.DCO2.GN.ZS, NY.ADJ.DRES.GN.ZS, NY.GDP.MKTP.KD.ZG.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class GreenGdpGap(LayerBase):
    layer_id = "lEA"
    name = "Green GDP Gap"

    async def compute(self, db, **kwargs) -> dict:
        # Fetch GDP growth rate
        gdp_code = "NY.GDP.MKTP.KD.ZG"
        gdp_name = "GDP growth"
        gdp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gdp_code, f"%{gdp_name}%"),
        )

        # Fetch CO2 damage
        co2_code = "NY.ADJ.DCO2.GN.ZS"
        co2_name = "CO2 damage"
        co2_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (co2_code, f"%{co2_name}%"),
        )

        # Fetch resource depletion
        dep_code = "NY.ADJ.DRES.GN.ZS"
        dep_name = "natural resource depletion"
        dep_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (dep_code, f"%{dep_name}%"),
        )

        if not gdp_rows or not (co2_rows or dep_rows):
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data for green GDP gap"}

        gdp_vals = [float(r["value"]) for r in gdp_rows if r["value"] is not None]
        co2_vals = [float(r["value"]) for r in co2_rows if r["value"] is not None]
        dep_vals = [float(r["value"]) for r in dep_rows if r["value"] is not None]

        if not gdp_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no GDP growth data"}

        gdp_growth = gdp_vals[0]
        env_cost = (co2_vals[0] if co2_vals else 0.0) + (dep_vals[0] if dep_vals else 0.0)
        # Approximate green GDP growth (env cost is % GNI; treat as approximate annual drag)
        green_gdp_growth = gdp_growth - env_cost * 0.1  # marginal annual adjustment
        gap = gdp_growth - green_gdp_growth  # = env_cost * 0.1

        # Score: gap near 0 -> 20, gap >= 5pp -> 85
        score = float(np.clip(20.0 + gap * 13.0, 10.0, 90.0))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "gdp_growth_pct": round(gdp_growth, 2),
                "green_gdp_growth_pct": round(green_gdp_growth, 2),
                "gap_pp": round(gap, 2),
                "env_cost_pct_gni": round(env_cost, 2),
            },
        }
