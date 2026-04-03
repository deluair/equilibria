"""Immigrant Entrepreneurship module.

Measures the entrepreneurial potential of immigrant communities by
combining remittance inflows (a proxy for diaspora economic ties and
available capital) with business formation ease. High remittances
flowing into a business-friendly environment signal strong conditions
for immigrant entrepreneurship and productive integration.

Lower score = better conditions (more entrepreneurship, less friction).
Score inverted: high remittances + easy business formation = low stress.

Score = clip(100 - (remittance_component + business_ease), 0, 100)

Sources: WDI (BX.TRF.PWKR.DT.GD.ZS, IC.BUS.NDNS.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ImmigrantEntrepreneurship(LayerBase):
    layer_id = "lMI"
    name = "Immigrant Entrepreneurship"

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

        biz_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IC.BUS.NDNS.ZS'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        if not remit_rows and not biz_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        remit_vals = [float(r["value"]) for r in remit_rows if r["value"] is not None]
        biz_vals = [float(r["value"]) for r in biz_rows if r["value"] is not None]

        if not remit_vals and not biz_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        remittance_pct = float(np.mean(remit_vals)) if remit_vals else 2.0
        biz_new_density = float(np.mean(biz_vals)) if biz_vals else 2.0

        # Remittance as capital source: higher = more entrepreneurial capital (positive)
        remit_component = float(np.clip(remittance_pct * 5, 0, 50))
        # Business density: higher = better environment (positive for integration)
        biz_component = float(np.clip(biz_new_density * 10, 0, 50))

        # Score = stress/barrier; higher entrepreneurship potential = lower stress
        raw = remit_component + biz_component
        score = float(np.clip(100 - raw, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "remittance_pct_gdp_avg": round(remittance_pct, 2),
            "new_business_density_avg": round(biz_new_density, 2),
            "components": {
                "remittance_capital": round(remit_component, 2),
                "business_environment": round(biz_component, 2),
            },
            "n_obs_remittance": len(remit_vals),
            "n_obs_business": len(biz_vals),
            "interpretation": (
                "low entrepreneurship conditions" if score > 65
                else "moderate conditions" if score > 35
                else "strong entrepreneurship environment"
            ),
        }
