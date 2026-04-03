"""Aging Social Burden module.

Demographic aging pressure on social systems.

Queries:
- 'SP.POP.DPND.OL' (age dependency ratio, old, % of working-age population)

High and rising old-age dependency ratio signals mounting stress on social protection systems.

Score = clip(old_age_ratio * 1.5, 0, 100)

Sources: WDI (SP.POP.DPND.OL)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class AgingSocialBurden(LayerBase):
    layer_id = "lSP"
    name = "Aging Social Burden"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.POP.DPND.OL'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        old_age_ratio = float(np.mean(values))
        latest = float(values[0])

        # Trend: is ratio rising?
        trend_direction = None
        if len(values) >= 5:
            recent = float(np.mean(values[:3]))
            older = float(np.mean(values[-3:]))
            trend_direction = "rising" if recent > older else "stable_or_declining"

        score = float(np.clip(old_age_ratio * 1.5, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "old_age_dependency_ratio": round(old_age_ratio, 2),
            "old_age_dependency_latest": round(latest, 2),
            "trend_direction": trend_direction,
            "n_obs": len(values),
            "period": f"{rows[-1]['date']} to {rows[0]['date']}",
            "interpretation": (
                "High old-age dependency ratio indicates mounting demographic pressure "
                "on pension and social protection systems."
            ),
            "_series": "SP.POP.DPND.OL",
            "_source": "WDI",
        }
