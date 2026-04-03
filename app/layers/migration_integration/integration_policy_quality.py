"""Integration Policy Quality module.

Assesses the quality of the institutional environment for migrant
integration by combining three World Bank governance estimates:
regulatory quality (RQ.EST), rule of law (RL.EST), and voice and
accountability (VA.EST). High scores across all three indicators
signal an effective policy environment that can design and implement
inclusive integration policies.

Score = clip(100 - composite_governance, 0, 100)

Sources: WDI / World Bank Governance Indicators (RQ.EST, RL.EST, VA.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class IntegrationPolicyQuality(LayerBase):
    layer_id = "lMI"
    name = "Integration Policy Quality"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rq_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'RQ.EST'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        rl_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'RL.EST'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        va_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'VA.EST'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        if not rq_rows and not rl_rows and not va_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        rq_vals = [float(r["value"]) for r in rq_rows if r["value"] is not None]
        rl_vals = [float(r["value"]) for r in rl_rows if r["value"] is not None]
        va_vals = [float(r["value"]) for r in va_rows if r["value"] is not None]

        if not rq_vals and not rl_vals and not va_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        rq_est = float(np.mean(rq_vals)) if rq_vals else 0.0
        rl_est = float(np.mean(rl_vals)) if rl_vals else 0.0
        va_est = float(np.mean(va_vals)) if va_vals else 0.0

        # Rescale each from [-2.5, 2.5] to [0, ~33]; sum caps at ~100
        rq_component = float(np.clip((rq_est + 2.5) / 5.0 * 33.3, 0, 33.3))
        rl_component = float(np.clip((rl_est + 2.5) / 5.0 * 33.3, 0, 33.3))
        va_component = float(np.clip((va_est + 2.5) / 5.0 * 33.3, 0, 33.3))

        composite = rq_component + rl_component + va_component
        # Low governance -> high score (worse policy environment)
        score = float(np.clip(100 - composite, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "regulatory_quality_est_avg": round(rq_est, 3),
            "rule_of_law_est_avg": round(rl_est, 3),
            "voice_accountability_est_avg": round(va_est, 3),
            "components": {
                "regulatory_quality": round(rq_component, 2),
                "rule_of_law": round(rl_component, 2),
                "voice_accountability": round(va_component, 2),
            },
            "n_obs_regulatory_quality": len(rq_vals),
            "n_obs_rule_of_law": len(rl_vals),
            "n_obs_voice_accountability": len(va_vals),
            "interpretation": (
                "poor integration policy environment" if score > 65
                else "moderate policy quality" if score > 35
                else "strong integration policy environment"
            ),
        }
