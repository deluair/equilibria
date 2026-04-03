"""Diaspora Network Strength module.

Measures the strength of diaspora economic networks by combining
remittance flows as a share of GDP with trade openness. High
remittances alongside high trade openness suggest an active diaspora
that facilitates both financial transfers and trade linkages, acting
as informal trade and investment ambassadors.

Score = clip(100 - (remit_component + trade_component), 0, 100)

Sources: WDI (BX.TRF.PWKR.DT.GD.ZS, NE.TRD.GNFS.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DiasporaNetworkStrength(LayerBase):
    layer_id = "lMI"
    name = "Diaspora Network Strength"

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

        trade_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.TRD.GNFS.ZS'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        if not remit_rows and not trade_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        remit_vals = [float(r["value"]) for r in remit_rows if r["value"] is not None]
        trade_vals = [float(r["value"]) for r in trade_rows if r["value"] is not None]

        if not remit_vals and not trade_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        remittance_pct = float(np.mean(remit_vals)) if remit_vals else 2.0
        trade_openness = float(np.mean(trade_vals)) if trade_vals else 50.0

        # Higher remittances = stronger diaspora financial network
        remit_component = float(np.clip(remittance_pct * 5, 0, 50))
        # Higher trade openness = stronger trade network facilitation
        trade_component = float(np.clip(trade_openness / 4, 0, 50))

        # Lower score = stronger diaspora network (less integration barrier)
        score = float(np.clip(100 - (remit_component + trade_component), 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "remittance_pct_gdp_avg": round(remittance_pct, 2),
            "trade_openness_pct_gdp_avg": round(trade_openness, 2),
            "components": {
                "remittance_network": round(remit_component, 2),
                "trade_facilitation": round(trade_component, 2),
            },
            "n_obs_remittance": len(remit_vals),
            "n_obs_trade": len(trade_vals),
            "interpretation": (
                "weak diaspora network" if score > 65
                else "moderate network" if score > 35
                else "strong diaspora network"
            ),
        }
