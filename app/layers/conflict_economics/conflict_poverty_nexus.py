"""Conflict Poverty Nexus module.

Measures the intensity of the poverty-conflict feedback loop. Combines
poverty headcount (SI.POV.DDAY) with political instability (PV.EST) to
assess how deeply poverty and conflict reinforce each other. High poverty
in politically unstable settings signals a reinforcing nexus.

Score = clip(nexus_index * 100, 0, 100).
High score = severe poverty-conflict feedback loop.

Sources: WDI (SI.POV.DDAY, PV.EST, NY.GDP.PCAP.KD)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ConflictPovertyNexus(LayerBase):
    layer_id = "lCW"
    name = "Conflict Poverty Nexus"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        poverty_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.POV.DDAY'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        stability_rows = await db.fetch_all(
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

        gdppc_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not poverty_rows and not stability_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        poverty_vals = [float(r["value"]) for r in poverty_rows if r["value"] is not None]
        stability_vals = [float(r["value"]) for r in stability_rows if r["value"] is not None]
        gdppc_vals = [float(r["value"]) for r in gdppc_rows if r["value"] is not None]

        if not poverty_vals and not stability_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        poverty_mean = float(np.mean(poverty_vals)) if poverty_vals else None
        stability_mean = float(np.mean(stability_vals)) if stability_vals else None
        gdppc_mean = float(np.mean(gdppc_vals)) if gdppc_vals else None

        # Poverty component: high headcount = high base score
        poverty_component = float(np.clip((poverty_mean / 100) * 50, 0, 50)) if poverty_mean is not None else 25.0

        # Instability multiplier: unstable + poor = strongest nexus
        if stability_mean is not None:
            instability_norm = 1.0 - (stability_mean + 2.5) / 5.0
            instability_component = float(np.clip(instability_norm * 40, 0, 40))
        else:
            instability_component = 20.0

        # Low income level amplifies nexus (poor countries more trapped)
        if gdppc_mean is not None:
            income_penalty = float(np.clip((1 - min(gdppc_mean / 10000, 1.0)) * 10, 0, 10))
        else:
            income_penalty = 0.0

        score = float(np.clip(poverty_component + instability_component + income_penalty, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "poverty_headcount_pct": round(poverty_mean, 4) if poverty_mean is not None else None,
            "political_stability_est": round(stability_mean, 4) if stability_mean is not None else None,
            "gdppc_mean": round(gdppc_mean, 2) if gdppc_mean is not None else None,
            "poverty_component": round(poverty_component, 2),
            "instability_component": round(instability_component, 2),
            "income_penalty": round(income_penalty, 2),
            "indicators": {
                "poverty_headcount": "SI.POV.DDAY",
                "political_stability": "PV.EST",
                "gdp_per_capita": "NY.GDP.PCAP.KD",
            },
        }
