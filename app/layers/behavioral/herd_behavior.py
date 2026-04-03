"""Herd Behavior module.

Investment volatility as a proxy for herd behavior and panic-driven investment cycles.
Uses gross capital formation (% of GDP) variance across years to detect herd/panic cycles.

High coefficient of variation in investment rates -> elevated herd behavior score.
Score = clip(cv * 100, 0, 100)

Source: WDI NE.GDI.TOTL.ZS (Gross capital formation, % of GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HerdBehavior(LayerBase):
    layer_id = "l13"
    name = "Herd Behavior"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.GDI.TOTL.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]

        mean_val = float(np.mean(values))
        std_val = float(np.std(values))

        if mean_val < 1e-10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "zero mean investment rate"}

        cv = std_val / mean_val
        score = float(np.clip(cv * 100, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(values),
            "period": f"{dates[0]} to {dates[-1]}",
            "gross_capital_formation_pct": {
                "mean": round(mean_val, 2),
                "std": round(std_val, 2),
                "cv": round(cv, 4),
                "min": round(float(np.min(values)), 2),
                "max": round(float(np.max(values)), 2),
            },
            "interpretation": "High CV in gross capital formation indicates herd/panic investment cycles",
        }
