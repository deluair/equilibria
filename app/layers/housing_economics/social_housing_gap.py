"""Social Housing Gap module.

Social and affordable housing supply shortfall proxy. Uses poverty headcount
ratio at $2.15/day (SI.POV.DDAY) and slum population share (EN.POP.SLUM.UR.ZS)
as need-side indicators. High poverty combined with high slum prevalence
signals a critical gap in social and affordable housing provision.

Score = clip((poverty_pct * 0.6) + (slum_pct * 0.8), 0, 100)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SocialHousingGap(LayerBase):
    layer_id = "lHO"
    name = "Social Housing Gap"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        poverty_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.POV.DDAY'
            ORDER BY dp.date
            """,
            (country,),
        )

        slum_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'EN.POP.SLUM.UR.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not poverty_rows or len(poverty_rows) < 1:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient poverty data for social housing gap analysis",
            }

        poverty_vals = np.array([float(r["value"]) for r in poverty_rows])
        poverty_pct = float(poverty_vals[-1])

        slum_pct = None
        slum_component = 0.0
        if slum_rows and len(slum_rows) >= 1:
            slum_vals = np.array([float(r["value"]) for r in slum_rows])
            slum_pct = float(slum_vals[-1])
            slum_component = slum_pct * 0.8

        raw_score = (poverty_pct * 0.6) + slum_component
        score = float(np.clip(raw_score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "poverty_headcount_pct": round(poverty_pct, 2),
            "slum_population_pct": round(slum_pct, 2) if slum_pct is not None else None,
            "gap_composite": round((poverty_pct * 0.6) + slum_component, 2),
            "n_poverty_obs": len(poverty_rows),
            "n_slum_obs": len(slum_rows) if slum_rows else 0,
            "methodology": "score = clip((poverty_pct * 0.6) + (slum_pct * 0.8), 0, 100)",
        }
