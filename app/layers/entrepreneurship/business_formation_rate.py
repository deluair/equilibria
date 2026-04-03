"""Business Formation Rate module.

Measures net business creation rate using:
- IC.BUS.NDNS.ZS: New business density (entry rate proxy)
- IC.BUS.DISC.XQ: Business discontinuance density (exit rate proxy, if available)

Net formation = entry rate - exit rate. High net formation indicates healthy
churn and entrepreneurial dynamism. Very low or negative net formation suggests
economic stagnation or regulatory suppression of new enterprise.

Score: higher score = lower net formation = more stress.

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class BusinessFormationRate(LayerBase):
    layer_id = "lER"
    name = "Business Formation Rate"

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
            return {"score": None, "signal": "UNAVAILABLE", "error": "no entry rate data"}

        entry_vals = [float(r["value"]) for r in entry_rows if r["value"] is not None]
        entry_rate = float(np.mean(entry_vals)) if entry_vals else None

        exit_rate: float | None = None
        if exit_rows:
            exit_vals = [float(r["value"]) for r in exit_rows if r["value"] is not None]
            exit_rate = float(np.mean(exit_vals)) if exit_vals else None

        if entry_rate is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient entry data"}

        net_formation = entry_rate - (exit_rate if exit_rate is not None else 0.0)

        # Net formation: typical range roughly -2 to +10 per 1,000 working-age pop.
        # Higher net formation = lower stress. Clamp to [0, 10] for normalization.
        norm = min(100.0, max(0.0, (net_formation / 10.0) * 100.0))
        score = max(0.0, 100.0 - norm)

        return {
            "score": round(score, 1),
            "country": country,
            "entry_rate_per_1000": round(entry_rate, 4),
            "exit_rate_per_1000": round(exit_rate, 4) if exit_rate is not None else None,
            "net_formation_rate": round(net_formation, 4),
            "interpretation": "High score = low net business creation = stressed entrepreneurial environment",
        }
