"""Refugee Economic Burden module.

Measures the fiscal and social cost of refugee flows using net migration
(SM.POP.NETM) and remittance outflows (BM.TRF.PWKR.CD.DT) as proxies
for displacement-driven population movements and associated resource flows.
High net emigration combined with fiscal pressure signals heavy burden.

Score = clip(burden_index * 100, 0, 100).
High score = severe refugee/displacement burden.

Sources: WDI (SM.POP.NETM, BM.TRF.PWKR.CD.DT, SP.POP.TOTL)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RefugeeEconomicBurden(LayerBase):
    layer_id = "lCW"
    name = "Refugee Economic Burden"

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
            LIMIT 10
            """,
            (country,),
        )

        pop_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.POP.TOTL'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        remit_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'BM.TRF.PWKR.CD.DT'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not migration_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        mig_vals = [float(r["value"]) for r in migration_rows if r["value"] is not None]
        if not mig_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        pop_vals = [float(r["value"]) for r in pop_rows if r["value"] is not None]
        pop_mean = float(np.mean(pop_vals)) if pop_vals else None

        # Net migration rate (negative = more emigration/displacement)
        net_mig_mean = float(np.mean(mig_vals))
        net_mig_rate = (net_mig_mean / pop_mean * 1000) if pop_mean and pop_mean > 0 else 0.0

        # Large negative net migration = displacement burden
        displacement_score = float(np.clip(abs(min(net_mig_rate, 0)) * 10, 0, 60))

        # Remittance outflow burden
        remit_vals = [float(r["value"]) for r in remit_rows if r["value"] is not None]
        remit_score = 0.0
        if remit_vals and pop_mean:
            remit_pc = float(np.mean(remit_vals)) / pop_mean
            remit_score = float(np.clip(remit_pc * 1e-5, 0, 40))

        score = float(np.clip(displacement_score + remit_score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "net_migration_mean": round(net_mig_mean, 0),
            "net_migration_rate_per_1000": round(net_mig_rate, 4),
            "population_mean": round(pop_mean, 0) if pop_mean else None,
            "remittance_outflow_mean_usd": round(float(np.mean(remit_vals)), 2) if remit_vals else None,
            "displacement_component": round(displacement_score, 2),
            "remittance_component": round(remit_score, 2),
            "n_obs": len(mig_vals),
            "indicators": {
                "net_migration": "SM.POP.NETM",
                "population": "SP.POP.TOTL",
                "remittance_outflows": "BM.TRF.PWKR.CD.DT",
            },
        }
