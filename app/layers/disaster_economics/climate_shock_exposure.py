"""Climate Shock Exposure module.

Measures macro-level sensitivity to climate shocks: agricultural GDP share
amplifies inflation volatility caused by climate disruptions.

Indicators:
  NV.AGR.TOTL.ZS   -- Agriculture, value added (% of GDP)
  FP.CPI.TOTL.ZG   -- Consumer price inflation (annual %)

Score = clip(ag_share_penalty + price_vol_penalty, 0, 100)
  ag_share_penalty: ag_share * 2      (higher ag share = more exposed)
  price_vol_penalty: price_cv * 30    (higher CPI volatility = more transmission)

Sources: WDI (NV.AGR.TOTL.ZS, FP.CPI.TOTL.ZG)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ClimateShockExposure(LayerBase):
    layer_id = "lDE"
    name = "Climate Shock Exposure"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        async def _fetch(series_id: str, limit: int = 20) -> list[float]:
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
                (country, series_id, limit),
            )
            return [float(r["value"]) for r in rows if r["value"] is not None]

        ag_vals = await _fetch("NV.AGR.TOTL.ZS")
        cpi_vals = await _fetch("FP.CPI.TOTL.ZG", limit=20)

        if not ag_vals and not cpi_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        ag_share = float(np.mean(ag_vals)) if ag_vals else 10.0
        cpi_arr = np.array(cpi_vals) if cpi_vals else np.array([5.0])

        cpi_mean = float(np.mean(np.abs(cpi_arr)))
        price_cv = float(np.std(cpi_arr) / cpi_mean) if cpi_mean > 1e-10 else 0.0

        ag_share_penalty = float(np.clip(ag_share * 2, 0, 60))
        price_vol_penalty = float(np.clip(price_cv * 30, 0, 40))
        score = float(np.clip(ag_share_penalty + price_vol_penalty, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "ag_share_pct_gdp": round(ag_share, 4),
            "cpi_volatility_cv": round(price_cv, 4),
            "ag_share_penalty": round(ag_share_penalty, 2),
            "price_vol_penalty": round(price_vol_penalty, 2),
            "indicators": {
                "ag_share": "NV.AGR.TOTL.ZS",
                "cpi_inflation": "FP.CPI.TOTL.ZG",
            },
        }
