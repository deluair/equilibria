"""Conflict GDP Loss module.

Measures GDP loss attributable to conflict and fragility using deviation
of actual GDP growth from potential growth in fragile/conflict-affected states.
Proxied via GDP growth volatility and below-potential growth episodes using
WDI NY.GDP.MKTP.KD.ZG and VC.IHR.PSRC.P5 (intentional homicides as fragility proxy).

Score = clip(loss_index * 100, 0, 100) where high score = severe GDP loss from conflict.

Sources: WDI (NY.GDP.MKTP.KD.ZG, VC.IHR.PSRC.P5)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ConflictGdpLoss(LayerBase):
    layer_id = "lCW"
    name = "Conflict GDP Loss"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        growth_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY dp.date DESC
            LIMIT 30
            """,
            (country,),
        )

        homicide_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'VC.IHR.PSRC.P5'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not growth_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        growth_vals = [float(r["value"]) for r in growth_rows if r["value"] is not None]
        if not growth_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        arr = np.array(growth_vals)
        growth_mean = float(np.mean(arr))
        growth_std = float(np.std(arr))

        # Growth shortfall below 2% threshold (minimal acceptable growth)
        threshold = 2.0
        shortfall_episodes = int(np.sum(arr < threshold))
        shortfall_ratio = shortfall_episodes / len(arr)

        # Homicide rate as conflict intensity proxy
        homicide_vals = [float(r["value"]) for r in homicide_rows if r["value"] is not None]
        homicide_mean = float(np.mean(homicide_vals)) if homicide_vals else None

        # Score components
        shortfall_component = float(np.clip(shortfall_ratio * 60, 0, 60))
        volatility_component = float(np.clip(growth_std * 3, 0, 25))
        homicide_component = float(np.clip((homicide_mean / 50) * 15, 0, 15)) if homicide_mean is not None else 0.0

        score = float(np.clip(shortfall_component + volatility_component + homicide_component, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "gdp_growth_mean": round(growth_mean, 4),
            "gdp_growth_std": round(growth_std, 4),
            "shortfall_episodes": shortfall_episodes,
            "shortfall_ratio": round(shortfall_ratio, 4),
            "homicide_rate_mean": round(homicide_mean, 4) if homicide_mean is not None else None,
            "n_obs": len(growth_vals),
            "shortfall_component": round(shortfall_component, 2),
            "volatility_component": round(volatility_component, 2),
            "homicide_component": round(homicide_component, 2),
            "indicators": {
                "gdp_growth": "NY.GDP.MKTP.KD.ZG",
                "homicides": "VC.IHR.PSRC.P5",
            },
        }
