"""Intergenerational Equity module.

Measures the pension burden on the working-age population. High old-age
dependency combined with low labor force participation reduces the effective
contributor base, amplifying the per-worker pension cost and creating
intergenerational equity stress.

Score = clip(dependency * max(0, 80 - lfp) / 20, 0, 100)

Sources: WDI SP.POP.DPND.OL (old-age dependency ratio, per 100 working-age),
         WDI SL.TLF.CACT.ZS (labor force participation rate, % of working-age)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class IntergenerationalEquity(LayerBase):
    layer_id = "lPS"
    name = "Intergenerational Equity"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        dependency_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.POP.DPND.OL'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        lfp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.TLF.CACT.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not dependency_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no dependency ratio data"}

        dep_vals = [float(r["value"]) for r in dependency_rows if r["value"] is not None]
        if not dep_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid dependency data"}

        dependency = float(np.mean(dep_vals))

        lfp_vals = [float(r["value"]) for r in lfp_rows if r["value"] is not None]
        lfp = float(np.mean(lfp_vals)) if lfp_vals else 60.0

        lfp_gap = max(0.0, 80.0 - lfp)
        score = float(np.clip(dependency * lfp_gap / 20.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "old_age_dependency_ratio": round(dependency, 2),
            "labor_force_participation_pct": round(lfp, 2),
            "lfp_gap_from_benchmark": round(lfp_gap, 2),
            "per_worker_burden_index": round(dependency * lfp_gap / 20.0, 3),
            "high_equity_stress": score > 50,
            "interpretation": (
                "severe intergenerational equity stress" if score > 75
                else "high intergenerational burden" if score > 50
                else "moderate intergenerational pressure" if score > 25
                else "intergenerational balance adequate"
            ),
            "sources": ["WDI SP.POP.DPND.OL", "WDI SL.TLF.CACT.ZS"],
        }
