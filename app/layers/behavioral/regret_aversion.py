"""Regret Aversion module.

Policy reversals: frequency of GDP growth direction changes.
Frequent sign changes in annual GDP growth rate indicate regret-driven policy lurching --
policymakers repeatedly reversing course, consistent with regret aversion theory.

Score = clip(reversals / n_years * 200, 0, 100)

Source: WDI NY.GDP.MKTP.KD.ZG (GDP growth, annual %)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RegretAversion(LayerBase):
    layer_id = "l13"
    name = "Regret Aversion"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]
        n_years = len(values)

        # Count sign changes (positive to negative or vice versa)
        signs = np.sign(values)
        sign_changes = np.sum(np.diff(signs) != 0)
        reversals = int(sign_changes)

        reversal_rate = reversals / n_years if n_years > 0 else 0.0
        score = float(np.clip(reversal_rate * 200, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": n_years,
            "period": f"{dates[0]} to {dates[-1]}",
            "growth_reversals": reversals,
            "reversal_rate": round(reversal_rate, 4),
            "mean_gdp_growth": round(float(np.mean(values)), 2),
            "std_gdp_growth": round(float(np.std(values)), 2),
            "n_contraction_years": int(np.sum(values < 0)),
            "interpretation": "Frequent growth sign changes indicate regret-driven policy reversals",
        }
