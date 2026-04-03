"""Housing Integration Stress module.

Measures housing pressure from immigration by combining net migration
volume with the share of urban population living in slums. High net
immigration into a country with a large slum population signals
acute housing integration stress: new arrivals are likely to crowd
into informal settlements, worsening urban poverty and health outcomes.

Score = clip((migration_norm * 0.5) + (slum_component * 0.5), 0, 100)

Sources: WDI (SM.POP.NETM, EN.POP.SLUM.UR.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HousingIntegrationStress(LayerBase):
    layer_id = "lMI"
    name = "Housing Integration Stress"

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

        slum_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'EN.POP.SLUM.UR.ZS'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        if not migration_rows and not slum_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        mig_vals = [float(r["value"]) for r in migration_rows if r["value"] is not None]
        slum_vals = [float(r["value"]) for r in slum_rows if r["value"] is not None]

        if not mig_vals and not slum_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        net_migration = float(np.mean(mig_vals)) if mig_vals else 0.0
        slum_pct = float(np.mean(slum_vals)) if slum_vals else 10.0

        mig_norm = float(np.clip(abs(net_migration) / 500_000 * 50, 0, 50))
        slum_component = float(np.clip(slum_pct * 2, 0, 50))

        score = float(np.clip(mig_norm * 0.5 + slum_component * 0.5, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "net_migration_avg": round(net_migration, 0),
            "slum_population_pct_avg": round(slum_pct, 2),
            "components": {
                "migration_pressure": round(mig_norm, 2),
                "housing_deficit": round(slum_component, 2),
            },
            "n_obs_migration": len(mig_vals),
            "n_obs_slum": len(slum_vals),
            "interpretation": (
                "severe housing stress" if score > 65
                else "moderate stress" if score > 35
                else "low stress"
            ),
        }
