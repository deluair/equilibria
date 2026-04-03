"""Venture Capital Depth module.

Measures VC investment as a percentage of GDP using World Bank WDI:
- CM.MKT.LCAP.GD.ZS: Market capitalization of listed domestic companies (% GDP)
  -- used as a capital market depth proxy when direct VC data is unavailable

Direct VC/GDP data is rarely available in standard open databases. Market
capitalization depth serves as a structural proxy: deeper capital markets
correlate strongly with active VC ecosystems. Low market cap relative to GDP
suggests limited risk capital availability for startups.

Score: higher score = shallower capital market / lower VC depth = more stress.

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class VentureCapitalDepth(LayerBase):
    layer_id = "lER"
    name = "Venture Capital Depth"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        mktcap_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'CM.MKT.LCAP.GD.ZS'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not mktcap_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no market capitalization data (CM.MKT.LCAP.GD.ZS)",
            }

        vals = [float(r["value"]) for r in mktcap_rows if r["value"] is not None]
        if not vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid values"}

        mktcap_pct_gdp = float(np.mean(vals))
        latest_year = mktcap_rows[0]["date"][:4] if mktcap_rows[0]["date"] else None

        # Market cap: 0-200%+ GDP. Frontier markets ~10-30%, developed ~80-150%.
        # Higher = better VC proxy. Clamp at 150%.
        norm = min(100.0, (mktcap_pct_gdp / 150.0) * 100.0)
        score = max(0.0, 100.0 - norm)

        return {
            "score": round(score, 1),
            "country": country,
            "market_cap_pct_gdp": round(mktcap_pct_gdp, 2),
            "latest_year": latest_year,
            "proxy_note": "Market capitalization used as VC depth proxy (direct VC/GDP not in WDI)",
            "interpretation": "High score = shallow capital markets = limited VC/risk capital depth",
        }
