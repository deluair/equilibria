"""Remittance Digitization module.

Remittance cost and digital channel proxy.
  - BX.TRF.PWKR.DT.GD.ZS: personal remittances received % GDP (WDI)
  - FX.OWN.TOTL.ZS: account ownership % adults (financial inclusion proxy)

High remittance dependency + low financial inclusion = expensive informal channels.

Score = clip(remittance_dep_score + exclusion_penalty, 0, 100).
  - remittance_dep_score = clip(remittance_pct * 4, 0, 60)
    (remittances >15% GDP => score 60)
  - exclusion_penalty = clip(max(0, 60 - ownership_pct) * 0.67, 0, 40)
    (0% ownership => penalty 40)

Source: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RemittanceDigitization(LayerBase):
    layer_id = "lDF"
    name = "Remittance Digitization"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        remit_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'BX.TRF.PWKR.DT.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        inclusion_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FX.OWN.TOTL.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not remit_rows and not inclusion_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        remit_vals = [float(r["value"]) for r in remit_rows if r["value"] is not None]
        inclusion_vals = [float(r["value"]) for r in inclusion_rows if r["value"] is not None]

        remit_mean = float(np.nanmean(remit_vals)) if remit_vals else None
        inclusion_mean = float(np.nanmean(inclusion_vals)) if inclusion_vals else None

        remit_dep_score = float(np.clip((remit_mean or 0) * 4.0, 0, 60)) if remit_mean is not None else 30.0
        exclusion_penalty = float(np.clip(max(0.0, 60.0 - (inclusion_mean or 60.0)) * 0.67, 0, 40)) if inclusion_mean is not None else 20.0

        score = float(np.clip(remit_dep_score + exclusion_penalty, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "remittances_pct_gdp": round(remit_mean, 2) if remit_mean is not None else None,
            "account_ownership_pct": round(inclusion_mean, 2) if inclusion_mean is not None else None,
            "remittance_dep_score": round(remit_dep_score, 2),
            "exclusion_penalty": round(exclusion_penalty, 2),
            "note": "Score 0 = digitized low-cost remittance channels. Score 100 = high dependency on costly informal.",
            "_citation": "World Bank WDI: BX.TRF.PWKR.DT.GD.ZS, FX.OWN.TOTL.ZS",
        }
