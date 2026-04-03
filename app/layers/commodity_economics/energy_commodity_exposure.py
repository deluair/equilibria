"""Energy Commodity Exposure module.

Measures a country's exposure to energy price shocks via its net energy
import bill and the pass-through of oil/gas price changes to GDP.

Methodology:
- Query energy imports as % GDP (EG.IMP.CONS.ZS or proxy via merchandise trade).
- Query oil price series (POILWTIUSDM).
- Compute oil price volatility (CoV over 24 months).
- Exposure score = energy_import_share_gdp * oil_price_cov * scaling.
- score = clip(energy_import_pct * 5 + oil_cov * 80, 0, 100).

Sources: World Bank WDI (EG.IMP.CONS.ZS), World Bank Pink Sheet (POILWTIUSDM).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class EnergyCommodityExposure(LayerBase):
    layer_id = "lCM"
    name = "Energy Commodity Exposure"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        # Energy import share of GDP
        energy_rows = await db.fetch_all(
            """
            SELECT dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'EG.IMP.CONS.ZS'
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        # Oil price series
        oil_rows = await db.fetch_all(
            """
            SELECT dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'POILWTIUSDM'
            ORDER BY dp.date DESC
            LIMIT 24
            """,
            (),
        )

        if not energy_rows and not oil_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no energy exposure data"}

        energy_import_pct = float(energy_rows[0]["value"]) if energy_rows else 5.0

        oil_cov = 0.0
        if len(oil_rows) >= 6:
            oil_vals = np.array([float(r["value"]) for r in oil_rows])
            oil_mean = float(np.mean(oil_vals))
            oil_std = float(np.std(oil_vals, ddof=1))
            oil_cov = oil_std / oil_mean if oil_mean > 0 else 0.0

        score = float(np.clip(energy_import_pct * 5 + oil_cov * 80, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "energy_import_pct_gdp": round(energy_import_pct, 3),
            "oil_price_cov": round(oil_cov, 4),
            "high_exposure": energy_import_pct > 10 or oil_cov > 0.3,
            "indicators": ["EG.IMP.CONS.ZS", "POILWTIUSDM"],
        }
