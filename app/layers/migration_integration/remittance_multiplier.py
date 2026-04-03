"""Remittance Multiplier module.

Estimates the demand-side multiplier effect of remittances on the
receiving economy by combining remittance inflows as a share of GDP
with household consumption growth. High remittances flowing into an
economy with strong consumption growth imply a significant multiplier:
transfers are being spent into the local economy rather than saved or
converted.

Score = clip(100 - (remit_component + consumption_component), 0, 100)

Sources: WDI (BX.TRF.PWKR.DT.GD.ZS, NE.CON.PRVT.KD.ZG)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RemittanceMultiplier(LayerBase):
    layer_id = "lMI"
    name = "Remittance Multiplier"

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
            LIMIT 15
            """,
            (country,),
        )

        consumption_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.CON.PRVT.KD.ZG'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        if not remit_rows and not consumption_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        remit_vals = [float(r["value"]) for r in remit_rows if r["value"] is not None]
        cons_vals = [float(r["value"]) for r in consumption_rows if r["value"] is not None]

        if not remit_vals and not cons_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        remittance_pct = float(np.mean(remit_vals)) if remit_vals else 2.0
        consumption_growth = float(np.mean(cons_vals)) if cons_vals else 2.0

        # Higher remittances = larger multiplier base (positive for integration)
        remit_component = float(np.clip(remittance_pct * 5, 0, 50))
        # Higher consumption growth = remittances being spent locally (positive)
        cons_component = float(np.clip(max(consumption_growth, 0) * 5, 0, 50))

        # Lower score = higher multiplier strength (less stress)
        score = float(np.clip(100 - (remit_component + cons_component), 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "remittance_pct_gdp_avg": round(remittance_pct, 2),
            "household_consumption_growth_avg_pct": round(consumption_growth, 2),
            "components": {
                "remittance_base": round(remit_component, 2),
                "consumption_transmission": round(cons_component, 2),
            },
            "n_obs_remittance": len(remit_vals),
            "n_obs_consumption": len(cons_vals),
            "interpretation": (
                "weak multiplier effect" if score > 65
                else "moderate multiplier" if score > 35
                else "strong remittance multiplier"
            ),
        }
