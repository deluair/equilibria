"""Metal and Mineral Dependence module.

Measures the degree to which a country's export revenues and fiscal position
depend on metal and mineral exports, creating exposure to commodity price cycles
and long-run resource depletion risks.

Methodology:
- Query ores and metals exports as % merchandise exports (TX.VAL.MMTL.ZS.UN).
- Query mineral rents as % GDP (NY.GDP.MINR.RT.ZS).
- Query coal rents as % GDP (NY.GDP.COAL.RT.ZS).
- Combine into a dependence index.
- score = clip(ore_export_share * 0.6 + (mineral_rents + coal_rents) * 3, 0, 100).

Sources: World Bank WDI (TX.VAL.MMTL.ZS.UN, NY.GDP.MINR.RT.ZS, NY.GDP.COAL.RT.ZS).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MetalMineralDependence(LayerBase):
    layer_id = "lCM"
    name = "Metal and Mineral Dependence"

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

        ore_export_share = await _latest("TX.VAL.MMTL.ZS.UN")
        mineral_rents = await _latest("NY.GDP.MINR.RT.ZS")
        coal_rents = await _latest("NY.GDP.COAL.RT.ZS")

        if ore_export_share is None and mineral_rents is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no metal/mineral data"}

        ore_share = ore_export_share or 0.0
        min_rents = mineral_rents or 0.0
        c_rents = coal_rents or 0.0

        score = float(np.clip(
            ore_share * 0.6 + (min_rents + c_rents) * 3.0,
            0,
            100,
        ))

        return {
            "score": round(score, 1),
            "country": country,
            "ore_metals_export_share_pct": round(ore_share, 3),
            "mineral_rents_pct_gdp": round(min_rents, 3),
            "coal_rents_pct_gdp": round(c_rents, 3),
            "high_dependence": ore_share > 30 or (min_rents + c_rents) > 5,
            "indicators": ["TX.VAL.MMTL.ZS.UN", "NY.GDP.MINR.RT.ZS", "NY.GDP.COAL.RT.ZS"],
        }
