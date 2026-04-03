"""Institutional Persistence module.

Measures governance stability over time via the standard deviation of the
Rule of Law (RL.EST) score across all available years. High volatility
indicates weak, fragile, or reversible institutions.

Indicator: RL.EST (World Governance Indicators, Rule of Law estimate).
Method: std dev of annual RL scores.
Score: high std dev -> high stress. std dev >= 0.5 mapped to score = 100.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_MAX_STD = 0.5  # std dev of RL at which score saturates at 100


class InstitutionalPersistence(LayerBase):
    layer_id = "lHI"
    name = "Institutional Persistence"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'RL.EST'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = np.array([float(r["value"]) for r in rows])
        std_dev = float(np.std(values, ddof=1))
        mean_rl = float(np.mean(values))
        latest_rl = float(values[-1])

        score = float(np.clip(std_dev / _MAX_STD * 100, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(rows),
            "period": f"{rows[0]['date'][:4]} to {rows[-1]['date'][:4]}",
            "rl_std_dev": round(std_dev, 4),
            "rl_mean": round(mean_rl, 4),
            "rl_latest": round(latest_rl, 4),
            "rl_min": round(float(np.min(values)), 4),
            "rl_max": round(float(np.max(values)), 4),
        }
