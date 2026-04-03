"""Immigrant Labor Integration module.

Measures how well immigrants are absorbed into the host labor market by
combining net migration volume with the unemployment rate. High net
immigration into a high-unemployment economy signals integration stress:
new arrivals compete for scarce jobs, depressing wages and raising
social tension.

Score = clip((net_migration_norm * 0.5) + (unemployment * 2), 0, 100)

Sources: WDI (SM.POP.NETM, SL.UEM.TOTL.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ImmigrantLaborIntegration(LayerBase):
    layer_id = "lMI"
    name = "Immigrant Labor Integration"

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

        unem_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.UEM.TOTL.ZS'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        if not migration_rows and not unem_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        mig_vals = [float(r["value"]) for r in migration_rows if r["value"] is not None]
        unem_vals = [float(r["value"]) for r in unem_rows if r["value"] is not None]

        if not mig_vals and not unem_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        net_migration = float(np.mean(mig_vals)) if mig_vals else 0.0
        unemployment = float(np.mean(unem_vals)) if unem_vals else 5.0

        # Normalize net migration: large positive = high immigration pressure
        mig_norm = float(np.clip(abs(net_migration) / 500_000 * 50, 0, 50))
        unem_component = float(np.clip(unemployment * 2, 0, 50))

        score = float(np.clip(mig_norm + unem_component, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "net_migration_avg": round(net_migration, 0),
            "unemployment_rate_avg_pct": round(unemployment, 2),
            "components": {
                "migration_pressure": round(mig_norm, 2),
                "unemployment_pressure": round(unem_component, 2),
            },
            "n_obs_migration": len(mig_vals),
            "n_obs_unemployment": len(unem_vals),
            "interpretation": (
                "high integration stress" if score > 65
                else "moderate stress" if score > 35
                else "manageable"
            ),
        }
