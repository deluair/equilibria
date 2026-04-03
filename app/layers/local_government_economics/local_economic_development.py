"""Local Economic Development module.

Measures the local economic development environment using GDP per capita
growth (NY.GDP.PCAP.KD.ZG) as a proxy for economic dynamism and the cost
of starting a business (IC.BUS.NDNS.ZS, cost as % of GNI per capita) as
a proxy for local regulatory burden on enterprise.

Score reflects development constraint: high score = constrained local development.
growth_stress = clip((5 - gdp_growth) / 10 * 100, 0, 100)
cost_stress = clip(cost_pct / 50 * 100, 0, 100)
Score = growth_stress * 0.5 + cost_stress * 0.5.

Sources: WDI NY.GDP.PCAP.KD.ZG, WDI IC.BUS.NDNS.ZS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_TARGET_GROWTH = 5.0      # % per year: benchmark adequate per-capita growth
_HIGH_COST_THRESHOLD = 50.0  # % GNI per capita: very high business start-up cost


class LocalEconomicDevelopment(LayerBase):
    layer_id = "lLG"
    name = "Local Economic Development"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        gdp_code = "NY.GDP.PCAP.KD.ZG"
        gdp_name = "GDP per capita growth"
        gdp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (gdp_code, f"%{gdp_name}%"),
        )

        biz_code = "IC.BUS.NDNS.ZS"
        biz_name = "cost of business start-up"
        biz_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (biz_code, f"%{biz_name}%"),
        )

        if not gdp_rows and not biz_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no local economic development data"}

        gdp_growth = float(gdp_rows[0]["value"]) if gdp_rows else None
        biz_cost = float(biz_rows[0]["value"]) if biz_rows else None

        growth_stress = float(np.clip((_TARGET_GROWTH - gdp_growth) / 10.0 * 100.0, 0, 100)) if gdp_growth is not None else 50.0
        cost_stress = float(np.clip(biz_cost / _HIGH_COST_THRESHOLD * 100.0, 0, 100)) if biz_cost is not None else 50.0

        score = float(np.clip(growth_stress * 0.5 + cost_stress * 0.5, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "gdp_per_capita_growth_pct": round(gdp_growth, 2) if gdp_growth is not None else None,
            "business_startup_cost_pct_gni": round(biz_cost, 2) if biz_cost is not None else None,
            "growth_stress_component": round(growth_stress, 1),
            "cost_stress_component": round(cost_stress, 1),
            "interpretation": (
                "Severely constrained local economic development"
                if score > 70
                else "Significant development constraints: low growth or high business costs" if score > 50
                else "Moderate development constraints" if score > 30
                else "Favorable local economic development environment"
            ),
            "_sources": ["WDI:NY.GDP.PCAP.KD.ZG", "WDI:IC.BUS.NDNS.ZS"],
        }
