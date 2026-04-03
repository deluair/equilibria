"""Technological Lock-In module.

Fossil fuel dependency lock-in: high and stubbornly persistent fossil fuel share
in the energy mix signals technological path dependency.

Score = clip(latest_fossil * 0.8 - slope * 5, 0, 100)
Stubbornly high fossil share (positive/flat trend) = high lock-in stress.
Declining trend (negative slope) reduces score.

Sources: WDI EG.USE.COMM.FO.ZS (fossil fuel energy consumption %)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TechnologicalLockIn(LayerBase):
    layer_id = "lCP"
    name = "Technological Lock-In"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'EG.USE.COMM.FO.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]

        latest_fossil = float(values[-1])

        # OLS slope of fossil share over time (pp per year)
        x = np.arange(len(values), dtype=float)
        slope = float(np.polyfit(x, values, 1)[0])

        # Negative slope = declining dependency = reduces lock-in score
        score = float(np.clip(latest_fossil * 0.8 - slope * 5.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "fossil_fuel_share_pct": round(latest_fossil, 2),
            "fossil_trend_slope_pp_per_year": round(slope, 4),
            "period": f"{dates[0]} to {dates[-1]}",
            "n_obs": len(values),
            "interpretation": (
                "High score = high fossil lock-in (high share, not declining). "
                "Low score = energy transition underway or already diversified."
            ),
            "_citation": "World Bank WDI: EG.USE.COMM.FO.ZS",
        }
