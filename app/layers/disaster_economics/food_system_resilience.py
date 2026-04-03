"""Food System Resilience module.

Measures agricultural resilience using crop production variability and
irrigation coverage. High crop production volatility combined with low
irrigation access signals a fragile food system.

Indicators:
  AG.PRD.CROP.XD -- Crop production index; std of annual changes = volatility
  AG.LND.IRIG.AG.ZS -- Agricultural land under irrigation (% of agricultural land)

Score = clip(volatility_penalty + irrigation_penalty, 0, 100)
  volatility_penalty: coefficient of variation (std/mean) * 50
  irrigation_penalty: max(0, 50 - irrigation_pct) * 1.0

Sources: WDI (AG.PRD.CROP.XD, AG.LND.IRIG.AG.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FoodSystemResilience(LayerBase):
    layer_id = "lDE"
    name = "Food System Resilience"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        crop_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'AG.PRD.CROP.XD'
            ORDER BY dp.date
            LIMIT 30
            """,
            (country,),
        )

        irr_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'AG.LND.IRIG.AG.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not crop_rows and not irr_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        crop_vals = np.array([float(r["value"]) for r in crop_rows if r["value"] is not None])
        irr_vals = [float(r["value"]) for r in irr_rows if r["value"] is not None]

        # Crop production volatility: coefficient of variation of annual changes
        if len(crop_vals) >= 5:
            changes = np.diff(crop_vals)
            mean_abs = float(np.mean(np.abs(crop_vals)))
            cv = float(np.std(changes) / mean_abs) if mean_abs > 1e-10 else 0.0
        else:
            cv = 0.0

        irrigation_pct = float(np.mean(irr_vals)) if irr_vals else 50.0

        volatility_penalty = float(np.clip(cv * 50, 0, 60))
        irrigation_penalty = max(0.0, 50.0 - irrigation_pct) * 1.0
        score = float(np.clip(volatility_penalty + irrigation_penalty, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "crop_production_cv": round(cv, 4),
            "irrigation_pct_agland": round(irrigation_pct, 4),
            "volatility_penalty": round(volatility_penalty, 2),
            "irrigation_penalty": round(irrigation_penalty, 2),
            "n_crop_obs": int(len(crop_vals)),
            "indicators": {
                "crop_production": "AG.PRD.CROP.XD",
                "irrigation": "AG.LND.IRIG.AG.ZS",
            },
        }
