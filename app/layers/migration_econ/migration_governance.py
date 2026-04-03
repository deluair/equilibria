"""Migration Governance module.

Assesses the quality of the migration policy environment using rule
of law and political stability as proxies for border management
capacity and migrant protection effectiveness.

Low rule of law undermines legal migration pathways, labor protections,
and bilateral migration agreements. Political instability further
erodes institutional capacity to manage migration flows.

Score = composite governance deficit; high score = poor migration
management environment.

Sources: WDI (RL.EST, PV.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MigrationGovernance(LayerBase):
    layer_id = "lME"
    name = "Migration Governance"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rl_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'RL.EST'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        pv_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'PV.EST'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not rl_rows and not pv_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        rl_vals = [float(r["value"]) for r in rl_rows if r["value"] is not None]
        pv_vals = [float(r["value"]) for r in pv_rows if r["value"] is not None]

        rl_est = float(np.mean(rl_vals)) if rl_vals else 0.0
        pv_est = float(np.mean(pv_vals)) if pv_vals else 0.0

        # Both RL.EST and PV.EST: -2.5 to +2.5. Lower = worse governance.
        # Convert deficit to 0-100 score. -2.5 = max stress (50 each).
        rl_deficit = max(0.0, -rl_est)
        pv_deficit = max(0.0, -pv_est)

        rl_score = float(np.clip(rl_deficit * 20, 0, 50))
        pv_score = float(np.clip(pv_deficit * 20, 0, 50))

        score = rl_score + pv_score

        return {
            "score": round(score, 1),
            "country": country,
            "rule_of_law_est": round(rl_est, 4),
            "political_stability_est": round(pv_est, 4),
            "components": {
                "rule_of_law_deficit": round(rl_score, 2),
                "instability_deficit": round(pv_score, 2),
            },
            "interpretation": (
                "poor migration governance" if score > 60
                else "weak governance" if score > 30
                else "adequate governance"
            ),
        }
