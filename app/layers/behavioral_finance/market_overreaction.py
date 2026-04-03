"""Market Overreaction module.

Excess stock market volatility relative to fundamentals, proxied by CPI inflation
volatility vs GDP growth volatility. High inflation variance with low GDP growth
signals market overreaction to nominal shocks.

Sources: WDI FP.CPI.TOTL.ZG (CPI inflation), NY.GDP.MKTP.KD.ZG (GDP growth)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MarketOverreaction(LayerBase):
    layer_id = "lBF"
    name = "Market Overreaction"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        cpi_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("FP.CPI.TOTL.ZG", "%CPI inflation%"),
        )
        gdp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("NY.GDP.MKTP.KD.ZG", "%GDP growth%"),
        )

        if not cpi_rows or len(cpi_rows) < 5 or not gdp_rows or len(gdp_rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        cpi_vals = np.array([float(r["value"]) for r in cpi_rows])
        gdp_vals = np.array([float(r["value"]) for r in gdp_rows])

        cpi_vol = float(np.std(cpi_vals))
        gdp_vol = float(np.std(gdp_vals))
        mean_gdp = float(np.mean(gdp_vals))

        # High CPI volatility relative to GDP volatility signals overreaction
        ratio = cpi_vol / (gdp_vol + 1e-6)
        # Penalize if mean GDP growth is low while CPI is volatile
        penalty = max(0.0, 3.0 - mean_gdp)
        score = float(np.clip(ratio * 15 + penalty * 5, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_cpi_obs": len(cpi_rows),
            "n_gdp_obs": len(gdp_rows),
            "cpi_volatility": round(cpi_vol, 3),
            "gdp_growth_volatility": round(gdp_vol, 3),
            "cpi_gdp_vol_ratio": round(ratio, 3),
            "mean_gdp_growth": round(mean_gdp, 3),
            "interpretation": "High CPI/GDP volatility ratio signals excess market overreaction to nominal shocks",
        }
