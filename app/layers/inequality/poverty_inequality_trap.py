"""Poverty-Inequality Trap module.

Measures the reinforcing dynamic between poverty headcount and income inequality.
When both poverty rates and Gini are simultaneously high, they create a
self-reinforcing trap: high inequality limits poverty reduction even during
growth (Bourguignon 2003, growth-inequality-poverty triangle).

Indicators:
- SI.POV.DDAY: Poverty headcount ratio at $2.15/day (2017 PPP, % of population)
- SI.POV.GINI: Gini index

Formula:
    score = clip(poverty_headcount / 2 + (gini - 30) / 1.4, 0, 100)

Components:
- poverty_headcount / 2: poverty at 100% -> 50 pts; 60% -> 30 pts
- (gini - 30) / 1.4: Gini 30 -> 0 pts; Gini 72 -> 30 pts; Gini 58 -> 20 pts
- Combined trap: 50 + 30 = 80 (CRISIS territory)

Sources: World Bank WDI (SI.POV.DDAY, SI.POV.GINI)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PovertyInequalityTrap(LayerBase):
    layer_id = "lIQ"
    name = "Poverty Inequality Trap"

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
            LIMIT 5
            """,
            (country,),
        )

        gini_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.POV.GINI'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not poverty_rows and not gini_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        poverty_headcount = float(poverty_rows[0]["value"]) if poverty_rows else 0.0
        gini = float(gini_rows[0]["value"]) if gini_rows else 35.0
        has_poverty = bool(poverty_rows)
        has_gini = bool(gini_rows)

        poverty_component = poverty_headcount / 2.0
        gini_component = max(0.0, (gini - 30.0) / 1.4)

        score = float(np.clip(poverty_component + gini_component, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "poverty_headcount_pct": round(poverty_headcount, 2),
            "gini": round(gini, 2),
            "poverty_source": "observed" if has_poverty else "imputed_default",
            "gini_source": "observed" if has_gini else "imputed_default",
            "poverty_component": round(poverty_component, 2),
            "gini_component": round(gini_component, 2),
            "interpretation": {
                "in_trap": poverty_headcount > 20 and gini > 40,
                "high_poverty": poverty_headcount > 20,
                "high_inequality": gini > 40,
                "trap_reference": "Bourguignon 2003: growth-inequality-poverty triangle",
            },
        }
