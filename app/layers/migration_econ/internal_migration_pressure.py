"""Internal Migration Pressure module.

Estimates rural-to-urban internal migration pressure from structural
mismatches: high agricultural employment share combined with a small
agricultural sector signals workforce displacement and urbanization
pressure.

When a large share of the labor force remains in agriculture but the
sector contributes little to GDP, workers are underemployed and
incentivized to migrate to cities. Rapid urban growth confirms
that this pressure is being realized.

Score = composite of agri employment-output gap and urban growth rate.

Sources: WDI (SP.URB.GROW, NV.AGR.TOTL.ZS, SL.AGR.EMPL.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class InternalMigrationPressure(LayerBase):
    layer_id = "lME"
    name = "Internal Migration Pressure"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        urban_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.URB.GROW'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        agri_gdp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NV.AGR.TOTL.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        agri_emp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.AGR.EMPL.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not urban_rows and not agri_gdp_rows and not agri_emp_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        urban_vals = [float(r["value"]) for r in urban_rows if r["value"] is not None]
        agri_gdp_vals = [float(r["value"]) for r in agri_gdp_rows if r["value"] is not None]
        agri_emp_vals = [float(r["value"]) for r in agri_emp_rows if r["value"] is not None]

        urban_growth = float(np.mean(urban_vals)) if urban_vals else 2.0
        agri_gdp = float(np.mean(agri_gdp_vals)) if agri_gdp_vals else 10.0
        agri_emp = float(np.mean(agri_emp_vals)) if agri_emp_vals else 20.0

        # Structural gap: high employment share, low GDP share = displacement pressure
        agri_gap = max(0.0, agri_emp - agri_gdp)
        gap_score = float(np.clip(agri_gap * 1.5, 0, 60))

        # Urban growth: rapid urbanization confirms migration is occurring
        urban_score = float(np.clip(urban_growth * 5, 0, 40))

        score = gap_score + urban_score

        return {
            "score": round(score, 1),
            "country": country,
            "urban_growth_rate_pct": round(urban_growth, 2),
            "agri_value_added_pct_gdp": round(agri_gdp, 2),
            "agri_employment_pct": round(agri_emp, 2),
            "employment_output_gap_pct": round(agri_gap, 2),
            "components": {
                "structural_gap_pressure": round(gap_score, 2),
                "urbanization_confirmation": round(urban_score, 2),
            },
            "interpretation": (
                "high internal migration pressure" if score > 65
                else "moderate pressure" if score > 40
                else "low pressure"
            ),
        }
