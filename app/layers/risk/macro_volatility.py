"""Macro Volatility module.

GDP and inflation volatility composite stress indicator.
Queries WDI:
  - NY.GDP.MKTP.KD.ZG : GDP growth, annual %
  - FP.CPI.TOTL.ZG    : CPI inflation, annual %

Method:
  1. Compute standard deviation of each series over available history.
  2. Composite volatility = sqrt((gdp_vol^2 + inf_vol^2) / 2).
  3. Normalize: composite vol > 10 = stress (score ~100).

Score = clip(composite_vol * 6, 0, 100)
  Example: gdp_vol=3%, inf_vol=4% -> composite=3.54 -> score=21 (STABLE)
           gdp_vol=5%, inf_vol=10% -> composite=7.91 -> score=47 (WATCH)
           gdp_vol=8%, inf_vol=15% -> composite=12.2 -> score=73 (STRESS)

Sources: World Bank WDI.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MacroVolatility(LayerBase):
    layer_id = "lRI"
    name = "Macro Volatility"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        async def fetch_series(series_id: str) -> list[float]:
            rows = await db.fetch_all(
                """
                SELECT dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                  AND dp.value IS NOT NULL
                ORDER BY dp.date
                """,
                (country, series_id),
            )
            return [float(r["value"]) for r in rows]

        gdp_vals = await fetch_series("NY.GDP.MKTP.KD.ZG")
        inf_vals = await fetch_series("FP.CPI.TOTL.ZG")

        if len(gdp_vals) < 5 and len(inf_vals) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data for both GDP and inflation",
            }

        gdp_vol = float(np.std(gdp_vals, ddof=1)) if len(gdp_vals) >= 5 else None
        inf_vol = float(np.std(inf_vals, ddof=1)) if len(inf_vals) >= 5 else None

        # Composite: root-mean-square volatility
        if gdp_vol is not None and inf_vol is not None:
            composite = float(np.sqrt((gdp_vol ** 2 + inf_vol ** 2) / 2.0))
        elif gdp_vol is not None:
            composite = gdp_vol
        else:
            composite = inf_vol  # type: ignore[assignment]

        score = float(np.clip(composite * 6.0, 0, 100))

        flags = []
        if gdp_vol is not None and gdp_vol > 5:
            flags.append(f"high GDP growth volatility: {gdp_vol:.2f}pp std dev")
        if inf_vol is not None and inf_vol > 8:
            flags.append(f"high inflation volatility: {inf_vol:.2f}pp std dev")

        return {
            "score": round(score, 1),
            "country": country,
            "gdp_volatility": round(gdp_vol, 4) if gdp_vol is not None else None,
            "inflation_volatility": round(inf_vol, 4) if inf_vol is not None else None,
            "composite_volatility": round(composite, 4),
            "n_gdp_obs": len(gdp_vals),
            "n_inflation_obs": len(inf_vals),
            "flags": flags,
        }
