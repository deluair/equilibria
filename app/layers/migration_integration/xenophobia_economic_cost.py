"""Xenophobia Economic Cost module.

Estimates the economic cost of social hostility toward migrants by
combining political stability (PV.EST) and voice/accountability
(VA.EST) governance indicators. Low political stability and weak
accountability create environments where xenophobia and discrimination
thrive, raising the economic cost of integration through reduced
productivity, social conflict, and brain drain.

Score = clip(100 - (stability_component + voice_component), 0, 100)

Sources: WDI / World Bank Governance Indicators (PV.EST, VA.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class XenophobiaEconomicCost(LayerBase):
    layer_id = "lMI"
    name = "Xenophobia Economic Cost"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        stability_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'PV.EST'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        voice_rows = await db.fetch_all(
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

        if not stability_rows and not voice_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        stab_vals = [float(r["value"]) for r in stability_rows if r["value"] is not None]
        voice_vals = [float(r["value"]) for r in voice_rows if r["value"] is not None]

        if not stab_vals and not voice_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        # WB governance estimates range roughly -2.5 to +2.5
        stability_est = float(np.mean(stab_vals)) if stab_vals else 0.0
        voice_est = float(np.mean(voice_vals)) if voice_vals else 0.0

        # Rescale from [-2.5, 2.5] to [0, 50]; higher governance = lower xenophobia cost
        stab_component = float(np.clip((stability_est + 2.5) / 5.0 * 50, 0, 50))
        voice_component = float(np.clip((voice_est + 2.5) / 5.0 * 50, 0, 50))

        # High governance -> low cost (invert)
        score = float(np.clip(100 - (stab_component + voice_component), 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "political_stability_est_avg": round(stability_est, 3),
            "voice_accountability_est_avg": round(voice_est, 3),
            "components": {
                "stability_governance": round(stab_component, 2),
                "voice_governance": round(voice_component, 2),
            },
            "n_obs_stability": len(stab_vals),
            "n_obs_voice": len(voice_vals),
            "interpretation": (
                "high xenophobia economic cost" if score > 65
                else "moderate cost" if score > 35
                else "low xenophobia cost"
            ),
        }
