"""Knowledge Spillovers module.

FDI as knowledge spillover channel: FDI inflows (% GDP) combined with
industry value-added share.

Low FDI with stagnant industry = limited knowledge spillovers.

Sources: WDI (BX.KLT.DINV.WD.GD.ZS, NV.IND.TOTL.ZS)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase

_FDI_LOW_THRESHOLD = 2.0  # % of GDP below which FDI is considered low


class KnowledgeSpillovers(LayerBase):
    layer_id = "lTE"
    name = "Knowledge Spillovers"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        fdi_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'BX.KLT.DINV.WD.GD.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )
        ind_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'NV.IND.TOTL.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not fdi_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no FDI inflow data",
            }

        fdi_vals = np.array([float(r["value"]) for r in fdi_rows])
        fdi_mean = float(np.mean(fdi_vals))
        fdi_latest = float(fdi_vals[-1])

        # FDI channel score: low FDI = limited spillovers (0-60 pts)
        fdi_score = float(np.clip(max(0.0, _FDI_LOW_THRESHOLD - fdi_mean) / _FDI_LOW_THRESHOLD * 60.0, 0.0, 60.0))

        # Industry stagnation score (0-40 pts)
        ind_trend_score = 0.0
        ind_slope = None
        if ind_rows and len(ind_rows) >= 5:
            ind_vals = np.array([float(r["value"]) for r in ind_rows])
            t = np.arange(len(ind_vals), dtype=float)
            slope, _, _, _, _ = linregress(t, ind_vals)
            ind_slope = float(slope)
            # Stagnant or declining industry = more limited spillover absorption
            if slope <= 0.5:
                ind_trend_score = float(np.clip((0.5 - slope) * 20.0, 0.0, 40.0))

        score = float(np.clip(fdi_score + ind_trend_score, 0.0, 100.0))

        result = {
            "score": round(score, 1),
            "country": country,
            "fdi_pct_gdp_latest": round(fdi_latest, 3),
            "fdi_pct_gdp_mean": round(fdi_mean, 3),
            "fdi_score_component": round(fdi_score, 1),
            "industry_trend_score_component": round(ind_trend_score, 1),
            "fdi_n_obs": len(fdi_rows),
            "interpretation": "low FDI + stagnant industry = limited knowledge spillovers",
        }
        if ind_slope is not None:
            result["industry_value_added_slope"] = round(ind_slope, 4)
        if ind_rows:
            result["industry_n_obs"] = len(ind_rows)

        return result
