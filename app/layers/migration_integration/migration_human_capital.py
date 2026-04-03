"""Migration Human Capital module.

Measures the human capital dimension of migration flows by combining
tertiary education enrollment (a proxy for host-country skill supply)
with net migration volume. High net immigration into a country with
high tertiary enrollment suggests migrants have access to a skilled
labour market and education system, favouring productive integration
and knowledge transfer.

Score = clip(100 - (edu_component + migration_boost), 0, 100)

Sources: WDI (SE.TER.ENRR, SM.POP.NETM)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MigrationHumanCapital(LayerBase):
    layer_id = "lMI"
    name = "Migration Human Capital"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        edu_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SE.TER.ENRR'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

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

        if not edu_rows and not migration_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        edu_vals = [float(r["value"]) for r in edu_rows if r["value"] is not None]
        mig_vals = [float(r["value"]) for r in migration_rows if r["value"] is not None]

        if not edu_vals and not mig_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        tertiary_enrr = float(np.mean(edu_vals)) if edu_vals else 30.0
        net_migration = float(np.mean(mig_vals)) if mig_vals else 0.0

        # Higher tertiary enrollment = stronger human capital environment (reduces gap)
        edu_component = float(np.clip(tertiary_enrr / 2, 0, 50))
        # Positive net migration into high-skill context boosts capital (reduces gap)
        mig_boost = float(np.clip(max(net_migration, 0) / 500_000 * 50, 0, 50))

        score = float(np.clip(100 - (edu_component + mig_boost), 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "tertiary_enrollment_avg_pct": round(tertiary_enrr, 2),
            "net_migration_avg": round(net_migration, 0),
            "components": {
                "education_environment": round(edu_component, 2),
                "migration_capital_boost": round(mig_boost, 2),
            },
            "n_obs_education": len(edu_vals),
            "n_obs_migration": len(mig_vals),
            "interpretation": (
                "weak human capital integration" if score > 65
                else "moderate integration" if score > 35
                else "strong human capital integration"
            ),
        }
