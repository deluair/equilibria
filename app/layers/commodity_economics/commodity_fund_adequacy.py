"""Commodity Fund Adequacy module.

Measures the adequacy of a country's sovereign wealth fund (SWF) or
commodity stabilization fund relative to commodity revenue volatility.
An adequate fund acts as a fiscal buffer during commodity price downturns.

Methodology:
- Query SWF assets as % GDP (proxy: NW.NCA.SAVM.CD / NY.GDP.MKTP.CD if available,
  or GC.AST.TOTL.GD.ZS as a fiscal asset proxy).
- Query commodity revenue volatility (std of NY.GDP.TOTL.RT.ZS over 10 years).
- Adequacy ratio = swf_assets_pct / max(commodity_rev_vol * 3, 1).
- score = clip((1 - min(adequacy_ratio, 1)) * 80 + resource_rents_level, 0, 100).
  Low fund relative to volatility = high vulnerability.

Sources: World Bank WDI (GC.AST.TOTL.GD.ZS, NY.GDP.TOTL.RT.ZS, NY.GDP.MKTP.KD.ZG).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CommodityFundAdequacy(LayerBase):
    layer_id = "lCM"
    name = "Commodity Fund Adequacy"

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

        async def _std(series_id: str, n: int = 10) -> float:
            rows = await db.fetch_all(
                """
                SELECT dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT ?
                """,
                (country, series_id, n),
            )
            if len(rows) < 3:
                return 2.0  # assume moderate volatility
            vals = [float(r["value"]) for r in rows]
            return float(np.std(vals, ddof=1))

        swf_proxy = await _latest("GC.AST.TOTL.GD.ZS")
        resource_rents = await _latest("NY.GDP.TOTL.RT.ZS")
        rents_vol = await _std("NY.GDP.TOTL.RT.ZS")

        if resource_rents is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no resource rents data"}

        rents = resource_rents
        swf = swf_proxy or 0.0
        vol = max(rents_vol, 0.5)

        # Required buffer: 3x annualized volatility
        required = vol * 3
        adequacy_ratio = swf / max(required, 1e-6)

        score = float(np.clip(
            (1 - min(adequacy_ratio, 1.0)) * 80 + min(rents / 10.0 * 20, 20),
            0,
            100,
        ))

        return {
            "score": round(score, 1),
            "country": country,
            "swf_assets_proxy_pct_gdp": round(swf, 3),
            "resource_rents_pct_gdp": round(rents, 3),
            "rents_volatility": round(vol, 3),
            "required_buffer_pct_gdp": round(required, 3),
            "adequacy_ratio": round(adequacy_ratio, 4),
            "underfunded": adequacy_ratio < 0.5,
            "indicators": ["GC.AST.TOTL.GD.ZS", "NY.GDP.TOTL.RT.ZS"],
        }
