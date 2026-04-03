"""Automation Displacement Risk module.

Share of jobs at high risk of automation.
Proxy: industry mix via NV.IND.MANF.ZS (manufacturing % GDP) and
SL.IND.EMPL.ZS (industry employment %) as structural indicators of automation exposure.

High manufacturing + industrial employment concentration = elevated automation displacement risk.

Score: higher score = higher automation displacement risk.

Source: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class AutomationDisplacementRisk(LayerBase):
    layer_id = "lDG"
    name = "Automation Displacement Risk"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        manf_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NV.IND.MANF.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        ind_emp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.IND.EMPL.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not manf_rows and not ind_emp_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no manufacturing/employment data"}

        manf_vals = [float(r["value"]) for r in manf_rows if r["value"] is not None]
        ind_emp_vals = [float(r["value"]) for r in ind_emp_rows if r["value"] is not None]

        manf_mean = float(np.nanmean(manf_vals)) if manf_vals else None
        ind_emp_mean = float(np.nanmean(ind_emp_vals)) if ind_emp_vals else None

        # Normalize: cap manufacturing at 40% GDP and industry employment at 60%
        manf_norm = float(np.clip((manf_mean or 0) / 40.0 * 100, 0, 100)) if manf_mean is not None else None
        ind_norm = float(np.clip((ind_emp_mean or 0) / 60.0 * 100, 0, 100)) if ind_emp_mean is not None else None

        components, weights = [], []
        if manf_norm is not None:
            components.append(manf_norm)
            weights.append(0.5)
        if ind_norm is not None:
            components.append(ind_norm)
            weights.append(0.5)

        if not components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable values"}

        total_w = sum(weights)
        risk = sum(c * w for c, w in zip(components, weights)) / total_w
        score = float(np.clip(risk, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "manufacturing_pct_gdp": round(manf_mean, 2) if manf_mean is not None else None,
            "industry_employment_pct": round(ind_emp_mean, 2) if ind_emp_mean is not None else None,
            "note": "Higher score = higher structural automation displacement risk.",
            "_citation": "World Bank WDI: NV.IND.MANF.ZS, SL.IND.EMPL.ZS",
        }
