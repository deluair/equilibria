"""Knowledge Diffusion module.

Measures the speed and momentum of technology diffusion by analyzing the
adoption curve of internet access over time (IT.NET.USER.ZS).

The second derivative (acceleration) of the adoption curve captures whether
diffusion is speeding up (positive) or decelerating (negative). Deceleration
indicates that the early-adopter phase has passed and diffusion is stalling,
which represents a knowledge transfer bottleneck.

Score is derived from normalized deceleration: stronger deceleration = higher score.

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class KnowledgeDiffusion(LayerBase):
    layer_id = "lNV"
    name = "Knowledge Diffusion"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IT.NET.USER.ZS'
              AND dp.value IS NOT NULL
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        dates = [r["date"] for r in rows]
        values = np.array([float(r["value"]) for r in rows])

        # First derivative: adoption rate (year-over-year change)
        first_deriv = np.diff(values)

        if len(first_deriv) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data for derivatives"}

        # Second derivative: acceleration of adoption
        second_deriv = np.diff(first_deriv)

        # Use recent acceleration (last 3 observations)
        recent_accel = float(np.mean(second_deriv[-3:]))
        overall_accel = float(np.mean(second_deriv))

        # Current internet penetration
        current_level = float(values[-1])
        recent_growth = float(np.mean(first_deriv[-3:])) if len(first_deriv) >= 3 else float(first_deriv[-1])

        # Deceleration score: negative acceleration = bottleneck
        # Normalize: -5 pp/yr^2 deceleration -> score 100; +5 pp/yr^2 -> score 0
        decel_score = max(0.0, min(100.0, (-recent_accel / 5.0) * 100.0 + 50.0))

        # Adjust for penetration level: low penetration + deceleration is worse
        penetration_penalty = max(0.0, (70.0 - current_level) / 70.0) * 20.0
        score = min(100.0, decel_score + penetration_penalty)

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(values),
            "period": f"{dates[0]} to {dates[-1]}",
            "current_internet_pct": round(current_level, 2),
            "recent_growth_pp_yr": round(recent_growth, 3),
            "recent_acceleration": round(recent_accel, 4),
            "overall_acceleration": round(overall_accel, 4),
            "interpretation": (
                "Negative acceleration = decelerating internet diffusion = knowledge transfer bottleneck"
            ),
        }
