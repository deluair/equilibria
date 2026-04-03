"""Firm Survival Rate module.

Measures 5-year firm survival rate as a proxy for entrepreneurial success and
ecosystem quality. Uses World Bank WDI:
- IC.BUS.NDNS.ZS: New business density -- used alongside discontinuance to estimate
  effective survival environment
- IC.BUS.DISC.XQ: Business discontinuance density (% working-age population)

When discontinuance rate is high relative to entry rate, the implied survival
environment is poor. A lower exit/entry ratio suggests more firms survive beyond
early years, indicating a healthier ecosystem and supportive business environment.

Score: higher score = higher relative exit rate = lower survival = more stress.

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FirmSurvivalRate(LayerBase):
    layer_id = "lER"
    name = "Firm Survival Rate"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        entry_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IC.BUS.NDNS.ZS'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        exit_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IC.BUS.DISC.XQ'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not entry_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no entry rate data for survival estimation"}

        entry_vals = [float(r["value"]) for r in entry_rows if r["value"] is not None]
        entry_rate = float(np.mean(entry_vals)) if entry_vals else None

        exit_rate: float | None = None
        if exit_rows:
            exit_vals = [float(r["value"]) for r in exit_rows if r["value"] is not None]
            exit_rate = float(np.mean(exit_vals)) if exit_vals else None

        if entry_rate is None or entry_rate == 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "zero entry rate, cannot compute ratio"}

        # If no exit data, use entry rate alone as a proxy: low entry = poor survival env.
        if exit_rate is None:
            norm = min(100.0, (entry_rate / 10.0) * 100.0)
            score = max(0.0, 100.0 - norm)
            return {
                "score": round(score, 1),
                "country": country,
                "entry_rate_per_1000": round(entry_rate, 4),
                "exit_rate_per_1000": None,
                "exit_entry_ratio": None,
                "data_note": "Exit rate unavailable; entry rate used as proxy",
                "interpretation": "High score = low entry rate = poor survival environment",
            }

        exit_entry_ratio = exit_rate / entry_rate

        # Ratio interpretation: 0.0 = no exits (ideal), 1.0 = all entrants exit, >1 = net decline.
        # Clamp ratio at 1.5 for scoring purposes.
        score = min(100.0, (exit_entry_ratio / 1.0) * 100.0)

        return {
            "score": round(score, 1),
            "country": country,
            "entry_rate_per_1000": round(entry_rate, 4),
            "exit_rate_per_1000": round(exit_rate, 4),
            "exit_entry_ratio": round(exit_entry_ratio, 4),
            "interpretation": "High score = exit rate approaches entry rate = low 5-year firm survival",
        }
