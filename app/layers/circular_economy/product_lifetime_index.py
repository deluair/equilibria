"""Product lifetime index: capital formation efficiency proxy.

Uses gross fixed capital formation as a share of GDP (NE.GDI.FTOT.ZS) and
GDP growth rate (NY.GDP.MKTP.KD.ZG) to infer capital efficiency. High capital
formation with low growth implies short product lifetimes (more replacement,
less durability). A more circular economy achieves more output per unit of
new capital invested.

References:
    Stahel, W.R. (2010). The Performance Economy (2nd ed.). Palgrave Macmillan.
    World Bank WDI: NE.GDI.FTOT.ZS, NY.GDP.MKTP.KD.ZG
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ProductLifetimeIndex(LayerBase):
    layer_id = "lCE"
    name = "Product Lifetime Index"

    GCF_CODE = "NE.GDI.FTOT.ZS"
    GDP_GROWTH_CODE = "NY.GDP.MKTP.KD.ZG"

    async def compute(self, db, **kwargs) -> dict:
        gcf_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (self.GCF_CODE, f"%{self.GCF_CODE}%"),
        )
        gdp_growth_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (self.GDP_GROWTH_CODE, f"%{self.GDP_GROWTH_CODE}%"),
        )

        if not gcf_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no gross capital formation data for product lifetime index",
            }

        gcf_vals = [r["value"] for r in gcf_rows if r["value"] is not None]
        if not gcf_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "null gross capital formation values",
            }

        gcf_latest = float(gcf_vals[0])

        gdp_growth_latest = None
        if gdp_growth_rows:
            gdp_growth_vals = [r["value"] for r in gdp_growth_rows if r["value"] is not None]
            if gdp_growth_vals:
                gdp_growth_latest = float(gdp_growth_vals[0])

        # Capital efficiency: growth per unit of capital formation
        # Higher ratio = longer product lifetimes, better circularity
        if gdp_growth_latest is not None and gcf_latest > 0:
            capital_efficiency = gdp_growth_latest / gcf_latest
        else:
            capital_efficiency = None

        # Trend in gross capital formation (rising = more replacement, shorter lifetimes)
        gcf_trend = None
        if len(gcf_vals) >= 3:
            arr = np.array(gcf_vals[:10], dtype=float)
            gcf_trend = float(np.polyfit(np.arange(len(arr)), arr, 1)[0])

        # Score: high GCF with low growth = shorter product lifetimes = higher stress
        # GCF benchmark: 20-25% of GDP is typical; above 30% with low growth = concern
        if capital_efficiency is not None:
            # Efficiency <0.2 implies poor capital productivity
            if capital_efficiency >= 0.4:
                raw_score = 15.0
            elif capital_efficiency >= 0.2:
                raw_score = 35.0
            elif capital_efficiency >= 0.0:
                raw_score = 60.0
            else:
                raw_score = 80.0  # negative growth
        else:
            # Fallback: high GCF alone
            raw_score = min(gcf_latest * 2.0, 100.0)

        score = float(np.clip(raw_score, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "gross_capital_formation_pct_gdp": round(gcf_latest, 2),
            "gdp_growth_rate_pct": round(gdp_growth_latest, 3) if gdp_growth_latest is not None else None,
            "capital_efficiency_ratio": round(capital_efficiency, 4) if capital_efficiency is not None else None,
            "gcf_trend_slope": round(gcf_trend, 4) if gcf_trend is not None else None,
        }
