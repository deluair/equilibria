"""Framing Effects module.

Budget framing: deficit vs surplus framing effect on fiscal behavior.
Persistent deficit framing (consistently negative cash balance) indicates
fiscal decisions locked into a deficit frame, consistent with framing effects theory.

Score based on persistence (fraction of years in deficit) and depth (mean deficit).

Source: WDI GC.BAL.CASH.GD.ZS (Cash surplus/deficit, % of GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FramingEffects(LayerBase):
    layer_id = "l13"
    name = "Framing Effects"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.BAL.CASH.GD.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]
        n = len(values)

        # Deficit persistence: fraction of years in deficit
        n_deficit = int(np.sum(values < 0))
        deficit_persistence = n_deficit / n

        # Deficit depth: mean deficit among deficit years (positive number = deeper)
        deficit_years = values[values < 0]
        mean_deficit_depth = float(np.mean(np.abs(deficit_years))) if len(deficit_years) > 0 else 0.0

        # Score: persistence (0-60) + depth (0-40)
        persistence_score = deficit_persistence * 60
        depth_score = float(np.clip(mean_deficit_depth * 4, 0, 40))
        score = float(np.clip(persistence_score + depth_score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": n,
            "period": f"{dates[0]} to {dates[-1]}",
            "n_deficit_years": n_deficit,
            "deficit_persistence": round(deficit_persistence, 4),
            "mean_deficit_depth_pct_gdp": round(mean_deficit_depth, 2),
            "mean_balance_pct_gdp": round(float(np.mean(values)), 2),
            "interpretation": "Persistent deficit framing indicates fiscal decisions locked into deficit frame",
        }
