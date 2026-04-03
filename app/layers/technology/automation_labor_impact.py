"""Automation and Labor Impact module.

Automation displacement risk proxy: manufacturing value-added share +
declining labor income share trend.

High manufacturing share combined with declining labor share = elevated
automation displacement risk.

Sources: WDI (NV.IND.MANF.ZS, SL.EMP.WORK.ZS as labor share proxy)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase

_MANF_HIGH_THRESHOLD = 20.0  # manufacturing % of GDP above which risk rises


class AutomationLaborImpact(LayerBase):
    layer_id = "lTE"
    name = "Automation Labor Impact"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        manf_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'NV.IND.MANF.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )
        labor_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'SL.EMP.WORK.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not manf_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no manufacturing value-added data",
            }

        manf_vals = np.array([float(r["value"]) for r in manf_rows])
        manf_latest = float(manf_vals[-1])

        # Manufacturing share contribution to risk (0-60 pts)
        manf_score = float(np.clip((manf_latest / _MANF_HIGH_THRESHOLD) * 60.0, 0.0, 60.0))

        # Labor share trend contribution (0-40 pts)
        labor_trend_score = 0.0
        labor_slope = None
        if labor_rows and len(labor_rows) >= 5:
            labor_vals = np.array([float(r["value"]) for r in labor_rows])
            t = np.arange(len(labor_vals), dtype=float)
            slope, _, _, _, _ = linregress(t, labor_vals)
            labor_slope = float(slope)
            # Declining labor share (negative slope) = higher displacement risk
            if slope < 0:
                labor_trend_score = float(np.clip(abs(slope) * 20.0, 0.0, 40.0))

        score = float(np.clip(manf_score + labor_trend_score, 0.0, 100.0))

        result = {
            "score": round(score, 1),
            "country": country,
            "manufacturing_share_pct_latest": round(manf_latest, 2),
            "manf_n_obs": len(manf_rows),
            "manf_score_component": round(manf_score, 1),
            "labor_trend_score_component": round(labor_trend_score, 1),
            "interpretation": (
                "high manufacturing + declining labor share = automation displacement risk"
            ),
        }
        if labor_slope is not None:
            result["labor_share_annual_slope"] = round(labor_slope, 4)
        if labor_rows:
            result["labor_n_obs"] = len(labor_rows)

        return result
