"""Resource Revenue Dependence module.

Measures the degree to which government fiscal revenues depend on commodity
(resource) revenues. Heavy dependence creates pro-cyclical fiscal policy and
vulnerability to commodity price busts.

Methodology:
- Query total natural resource rents as % GDP (NY.GDP.TOTL.RT.ZS).
- Query government revenue as % GDP (GC.REV.TOTL.GD.ZS).
- Resource revenue share of fiscal = resource_rents / gov_revenue (proxy).
- Score = clip(resource_share * 150 + max(0, resource_rents - 10) * 0.5, 0, 100).
  Resource share > 0.5 (50% of revenue) signals high fiscal vulnerability.

Sources: World Bank WDI (NY.GDP.TOTL.RT.ZS, GC.REV.TOTL.GD.ZS).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ResourceRevenueDependence(LayerBase):
    layer_id = "lCM"
    name = "Resource Revenue Dependence"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        async def _latest(series_id: str) -> float | None:
            rows = await db.fetch_all(
                """
                SELECT dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country, series_id),
            )
            return float(rows[0]["value"]) if rows else None

        resource_rents = await _latest("NY.GDP.TOTL.RT.ZS")
        gov_revenue = await _latest("GC.REV.TOTL.GD.ZS")

        if resource_rents is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no resource rents data"}

        rents = resource_rents
        gov_rev = gov_revenue or 25.0  # assume 25% GDP if missing

        resource_share = rents / max(gov_rev, 1e-6)

        score = float(np.clip(
            resource_share * 150 + max(0.0, rents - 10.0) * 0.5,
            0,
            100,
        ))

        return {
            "score": round(score, 1),
            "country": country,
            "resource_rents_pct_gdp": round(rents, 3),
            "gov_revenue_pct_gdp": round(gov_rev, 3),
            "resource_share_of_revenue": round(resource_share, 4),
            "high_fiscal_dependence": resource_share > 0.3,
            "indicators": ["NY.GDP.TOTL.RT.ZS", "GC.REV.TOTL.GD.ZS"],
        }
