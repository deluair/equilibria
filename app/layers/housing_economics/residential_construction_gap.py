"""Residential Construction Gap module.

Proxies housing supply-demand deficit via urban population growth rate
(SP.URB.GROW) against gross fixed capital formation (NE.GDI.FTOT.ZS) as
a construction investment signal. Rapid urbanization with low capital
formation implies supply cannot keep pace with demand.

Score = clip((urban_growth * 10) - (gfcf_pct * 0.5) + 25, 0, 100)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ResidentialConstructionGap(LayerBase):
    layer_id = "lHO"
    name = "Residential Construction Gap"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        urban_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.URB.GROW'
            ORDER BY dp.date
            """,
            (country,),
        )

        gfcf_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.GDI.FTOT.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not urban_rows or len(urban_rows) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient urban growth data for construction gap analysis",
            }

        if not gfcf_rows or len(gfcf_rows) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient capital formation data for construction gap analysis",
            }

        urban_vals = np.array([float(r["value"]) for r in urban_rows])
        gfcf_vals = np.array([float(r["value"]) for r in gfcf_rows])

        urban_growth = float(np.mean(urban_vals[-3:])) if len(urban_vals) >= 3 else float(urban_vals[-1])
        gfcf_pct = float(np.mean(gfcf_vals[-3:])) if len(gfcf_vals) >= 3 else float(gfcf_vals[-1])

        raw_score = (urban_growth * 10) - (gfcf_pct * 0.5) + 25
        score = float(np.clip(raw_score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "urban_growth_rate_pct": round(urban_growth, 2),
            "gross_fixed_capital_formation_pct_gdp": round(gfcf_pct, 2),
            "supply_demand_gap_signal": round((urban_growth * 10) - (gfcf_pct * 0.5), 2),
            "n_urban_obs": len(urban_rows),
            "n_gfcf_obs": len(gfcf_rows),
            "methodology": "score = clip((urban_growth * 10) - (gfcf_pct * 0.5) + 25, 0, 100)",
        }
