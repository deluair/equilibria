"""Creative Destruction Index module.

Measures Schumpeterian creative destruction intensity as:
  (entry rate + exit rate) / total firm stock proxy

High combined churn (entry + exit relative to base) indicates active reallocation
of resources from unproductive to productive uses. Low churn signals economic
calcification: incumbents persist, new entrants are deterred, and factor markets
are rigid.

Uses World Bank WDI:
- IC.BUS.NDNS.ZS: New business density (entry rate per 1,000 working-age pop)
- IC.BUS.DISC.XQ: Business discontinuance density (exit rate per 1,000 working-age pop)

Score: higher score = lower churn = less creative destruction = more stress.

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CreativeDestructionIndex(LayerBase):
    layer_id = "lER"
    name = "Creative Destruction Index"

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
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid entry rate values"}

        # Churn = entry + exit. Use entry only if exit unavailable.
        churn = entry_rate + (exit_rate if exit_rate is not None else 0.0)

        # Churn range: 0 to ~20 per 1,000. Clamp at 15 for normalization.
        norm = min(100.0, (churn / 15.0) * 100.0)
        score = max(0.0, 100.0 - norm)

        return {
            "score": round(score, 1),
            "country": country,
            "entry_rate_per_1000": round(entry_rate, 4),
            "exit_rate_per_1000": round(exit_rate, 4) if exit_rate is not None else None,
            "churn_rate": round(churn, 4),
            "interpretation": "High score = low churn = low creative destruction = rigid economy",
        }
