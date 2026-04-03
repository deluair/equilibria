"""Availability Heuristic module.

Recency bias in investment: boom-bust FDI cycles.
Detects sharp oscillations (volatility spikes) in FDI inflows as a proxy for
availability heuristic -- investors over-reacting to recent salient events.

Score = clip(max_pct_swing / 100 * 50, 0, 100)

Source: WDI BX.KLT.DINV.WD.GD.ZS (Foreign direct investment, net inflows, % of GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class AvailabilityHeuristic(LayerBase):
    layer_id = "l13"
    name = "Availability Heuristic"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'BX.KLT.DINV.WD.GD.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]

        # Year-over-year percent swings
        pct_changes = []
        for i in range(1, len(values)):
            prev = values[i - 1]
            curr = values[i]
            if abs(prev) > 1e-10:
                pct_changes.append(abs((curr - prev) / abs(prev)) * 100)

        if not pct_changes:
            return {"score": None, "signal": "UNAVAILABLE", "error": "cannot compute swings"}

        pct_arr = np.array(pct_changes)
        max_pct_swing = float(np.max(pct_arr))
        mean_pct_swing = float(np.mean(pct_arr))

        score = float(np.clip(max_pct_swing / 100 * 50, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(values),
            "period": f"{dates[0]} to {dates[-1]}",
            "fdi_pct_gdp": {
                "mean": round(float(np.mean(values)), 2),
                "std": round(float(np.std(values)), 2),
            },
            "max_yoy_pct_swing": round(max_pct_swing, 2),
            "mean_yoy_pct_swing": round(mean_pct_swing, 2),
            "interpretation": "Large FDI oscillations indicate availability heuristic / recency bias in investment",
        }
