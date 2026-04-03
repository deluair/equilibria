"""Reshoring Pressure module.

Detects signals that GVC activity previously offshored to this country
may be returning to originating economies or relocating elsewhere.

Two drivers:

1. **FDI trend** (BX.KLT.DINV.WD.GD.ZS): declining inward FDI as % GDP
   signals waning attractiveness for offshored production. A negative
   slope over the panel indicates outflows or drying-up of new investment.

2. **Labor productivity** proxy (SL.GDP.PCAP.EM.KD): GDP per person employed
   (constant 2017 USD). Rising trend = rising effective labor costs relative
   to productivity baseline, making the country less competitive for
   cost-driven GVC positioning.

Score:
  fdi_score = clip(50 - fdi_slope * 500, 0, 50)  -- declining FDI = stress
  wage_score = clip(wage_slope * 500, 0, 50)       -- rising wages = pressure
  total = fdi_score + wage_score

Sources: World Bank WDI (BX.KLT.DINV.WD.GD.ZS, SL.GDP.PCAP.EM.KD).
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class ReshoringPressure(LayerBase):
    layer_id = "lVC"
    name = "Reshoring Pressure"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        fdi_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'BX.KLT.DINV.WD.GD.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        wage_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.GDP.PCAP.EM.KD'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not fdi_rows or len(fdi_rows) < 4:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient FDI data"}

        fdi_vals = np.array([float(r["value"]) for r in fdi_rows])
        fdi_dates = [r["date"] for r in fdi_rows]
        x_fdi = np.arange(len(fdi_vals), dtype=float)
        fdi_slope, _, fdi_r2, fdi_p, _ = linregress(x_fdi, fdi_vals)
        fdi_slope = float(fdi_slope)

        wage_slope = None
        wage_dates = None
        wage_vals = None
        if wage_rows and len(wage_rows) >= 4:
            wage_vals = np.array([float(r["value"]) for r in wage_rows])
            wage_dates = [r["date"] for r in wage_rows]
            x_w = np.arange(len(wage_vals), dtype=float)
            # Normalize slope relative to mean to get fractional change per year
            mean_wage = float(np.mean(wage_vals))
            raw_slope, _, _, _, _ = linregress(x_w, wage_vals)
            wage_slope = float(raw_slope) / mean_wage if mean_wage > 0 else 0.0

        # Scores
        fdi_score = float(np.clip(50.0 - fdi_slope * 500.0, 0.0, 50.0))
        if wage_slope is not None:
            wage_score = float(np.clip(wage_slope * 500.0, 0.0, 50.0))
        else:
            wage_score = 25.0  # neutral

        score = fdi_score + wage_score

        return {
            "score": round(score, 1),
            "country": country,
            "fdi_trend_slope_pct_gdp_per_yr": round(fdi_slope, 5),
            "fdi_score": round(fdi_score, 1),
            "fdi_period": f"{fdi_dates[0]} to {fdi_dates[-1]}",
            "mean_fdi_pct_gdp": round(float(np.mean(fdi_vals)), 2),
            "labor_productivity_rel_slope": round(wage_slope, 6) if wage_slope is not None else None,
            "wage_score": round(wage_score, 1),
            "n_obs_fdi": len(fdi_vals),
            "n_obs_wages": len(wage_vals) if wage_vals is not None else 0,
            "interpretation": (
                "strong reshoring pressure" if score > 65
                else "moderate reshoring risk" if score > 40
                else "low reshoring pressure"
            ),
        }
