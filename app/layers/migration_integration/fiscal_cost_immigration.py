"""Fiscal Cost of Immigration module.

Estimates the fiscal pressure from immigration by combining migration
volume with social transfer expenditure as a share of GDP. High net
immigration into a country with large social transfer systems can imply
significant fiscal exposure if migrants access benefits before
contributing to the tax base.

Score = clip((migration_norm * 0.5) + (transfers_pct * 3), 0, 100)

Sources: WDI (SM.POP.NETM, GC.XPN.TRFT.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FiscalCostImmigration(LayerBase):
    layer_id = "lMI"
    name = "Fiscal Cost of Immigration"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        migration_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SM.POP.NETM'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        transfer_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.XPN.TRFT.ZS'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        if not migration_rows and not transfer_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        mig_vals = [float(r["value"]) for r in migration_rows if r["value"] is not None]
        transfer_vals = [float(r["value"]) for r in transfer_rows if r["value"] is not None]

        if not mig_vals and not transfer_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        net_migration = float(np.mean(mig_vals)) if mig_vals else 0.0
        transfers_pct = float(np.mean(transfer_vals)) if transfer_vals else 10.0

        mig_norm = float(np.clip(abs(net_migration) / 500_000 * 50, 0, 50))
        transfer_component = float(np.clip(transfers_pct * 3, 0, 50))

        score = float(np.clip(mig_norm + transfer_component, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "net_migration_avg": round(net_migration, 0),
            "social_transfers_pct_gdp_avg": round(transfers_pct, 2),
            "components": {
                "migration_volume": round(mig_norm, 2),
                "transfer_exposure": round(transfer_component, 2),
            },
            "n_obs_migration": len(mig_vals),
            "n_obs_transfers": len(transfer_vals),
            "interpretation": (
                "high fiscal exposure" if score > 65
                else "moderate exposure" if score > 35
                else "low exposure"
            ),
        }
