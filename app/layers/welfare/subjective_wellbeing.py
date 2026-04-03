"""Subjective Wellbeing module.

Constructs an economic conditions proxy for subjective wellbeing using:
  - NY.GDP.PCAP.KD.ZG : income growth (% YoY)
  - SI.POV.GINI        : inequality (Gini coefficient)
  - SL.UEM.TOTL.ZS    : unemployment rate (% of total labor force)

Weighted stress composite: high unemployment + rising inequality + low/negative
income growth = elevated wellbeing stress.

Score = weighted sum of penalized components, clipped to [0, 100].

Sources: WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SubjectiveWellbeing(LayerBase):
    layer_id = "lWE"
    name = "Subjective Wellbeing"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        series_map = {
            "income_growth": "NY.GDP.PCAP.KD.ZG",
            "gini": "SI.POV.GINI",
            "unemployment": "SL.UEM.TOTL.ZS",
        }

        latest: dict[str, float | None] = {}
        dates: dict[str, str | None] = {}

        for key, sid in series_map.items():
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country, sid),
            )
            if rows:
                latest[key] = float(rows[0]["value"])
                dates[key] = rows[0]["date"]
            else:
                latest[key] = None
                dates[key] = None

        available = {k: v for k, v in latest.items() if v is not None}
        if len(available) == 0:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no wellbeing proxy data available",
            }

        components: dict[str, float] = {}

        # Income growth: negative or low growth -> stress
        if latest["income_growth"] is not None:
            g = latest["income_growth"]
            # Penalty: growth below 2% raises stress, max 40 points
            components["income_growth_penalty"] = float(np.clip((2.0 - g) * 5.0, 0, 40))

        # Inequality: Gini above 30 raises stress, max 35 points
        if latest["gini"] is not None:
            components["inequality_penalty"] = float(np.clip((latest["gini"] - 30) * 0.7, 0, 35))

        # Unemployment: above 5% raises stress, max 25 points
        if latest["unemployment"] is not None:
            components["unemployment_penalty"] = float(np.clip((latest["unemployment"] - 5) * 1.5, 0, 25))

        if not components:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data for stress composite",
            }

        score = float(np.clip(sum(components.values()), 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "income_growth_pct": round(latest["income_growth"], 2) if latest["income_growth"] is not None else None,
            "income_growth_date": dates["income_growth"],
            "gini": round(latest["gini"], 2) if latest["gini"] is not None else None,
            "gini_date": dates["gini"],
            "unemployment_pct": round(latest["unemployment"], 2) if latest["unemployment"] is not None else None,
            "unemployment_date": dates["unemployment"],
            "stress_components": {k: round(v, 2) for k, v in components.items()},
            "method": "Weighted penalty composite: income growth, inequality, unemployment stress",
            "reference": "Easterlin 1974; Diener & Biswas-Diener 2002; Clark et al. 2008",
        }
