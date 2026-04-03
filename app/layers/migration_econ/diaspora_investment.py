"""Diaspora Investment module.

Compares remittance inflows to FDI inflows as a share of GDP to
assess whether the diaspora rather than foreign investors is the
primary source of external capital.

When remittances significantly exceed FDI, it indicates that the
diaspora is filling the investment gap, but the capital tends toward
household consumption rather than productive investment.

Score reflects the degree to which diaspora remittances dominate
over FDI, signaling structural capital access weakness.

Sources: WDI (BX.TRF.PWKR.DT.GD.ZS, BX.KLT.DINV.WD.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DiasporaInvestment(LayerBase):
    layer_id = "lME"
    name = "Diaspora Investment"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rem_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'BX.TRF.PWKR.DT.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        fdi_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'BX.KLT.DINV.WD.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not rem_rows and not fdi_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        rem_vals = [float(r["value"]) for r in rem_rows if r["value"] is not None]
        fdi_vals = [float(r["value"]) for r in fdi_rows if r["value"] is not None]

        rem = float(np.mean(rem_vals)) if rem_vals else 0.0
        fdi = float(np.mean(fdi_vals)) if fdi_vals else 0.0

        # Ratio of remittances to FDI (cap at 10x)
        if fdi > 0.01:
            rem_fdi_ratio = rem / fdi
        elif rem > 0:
            rem_fdi_ratio = 10.0  # FDI negligible, remittances positive
        else:
            rem_fdi_ratio = 1.0

        rem_fdi_ratio = float(np.clip(rem_fdi_ratio, 0, 10))

        # Score: high ratio -> high diaspora dependency (stress)
        # ratio of 1 = equal -> 50 score; ratio 5+ -> high stress
        score = float(np.clip(rem_fdi_ratio * 10, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "remittance_pct_gdp": round(rem, 2),
            "fdi_pct_gdp": round(fdi, 2),
            "remittance_to_fdi_ratio": round(rem_fdi_ratio, 2),
            "interpretation": (
                "diaspora is primary capital source" if rem_fdi_ratio > 3
                else "mixed capital sources" if rem_fdi_ratio > 1
                else "FDI dominates over remittances"
            ),
        }
