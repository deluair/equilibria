"""Undernourishment prevalence as a direct food security indicator.

The prevalence of undernourishment (PoU) is the FAO's primary headline
indicator of food insecurity, measuring the proportion of the population
whose habitual food consumption is insufficient to provide dietary energy
levels required to maintain a normal, active, healthy life.

Methodology:
    Fetch WDI indicator SN.ITK.DEFC.ZS (prevalence of undernourishment, %).

    score = clip(undernourishment_pct * 2, 0, 100)

Interpretation:
    - <5%: negligible (FAO threshold for "food secure" countries)
    - 5-10%: concern (score 10-20)
    - 10-25%: moderate stress (score 20-50)
    - 25-50%: severe stress (score 50-100)
    - >50%: crisis (score = 100)

Score (0-100): Higher score = greater undernourishment stress.

References:
    FAO, IFAD, UNICEF, WFP, WHO (2022). "The State of Food Security and
        Nutrition in the World." Rome: FAO.
    World Bank (2023). WDI: SN.ITK.DEFC.ZS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class UndernourishmentGap(LayerBase):
    layer_id = "lFS"
    name = "Undernourishment Gap"

    async def compute(self, db, **kwargs) -> dict:
        """Compute undernourishment prevalence stress score.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
        """
        country = kwargs.get("country_iso3", "BGD")

        row = await db.fetch_one(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.indicator_code = 'SN.ITK.DEFC.ZS'
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )
        if not row:
            row = await db.fetch_one(
                """
                SELECT dp.value, dp.date
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE '%undernourishment%'
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country,),
            )

        if not row or row["value"] is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no undernourishment prevalence data available",
            }

        und_pct = float(row["value"])
        score = float(np.clip(und_pct * 2.0, 0.0, 100.0))

        severity = (
            "negligible" if und_pct < 5.0
            else "concern" if und_pct < 10.0
            else "moderate" if und_pct < 25.0
            else "severe" if und_pct < 50.0
            else "crisis"
        )

        return {
            "score": round(score, 2),
            "country": country,
            "undernourishment_pct": round(und_pct, 2),
            "severity": severity,
            "fao_threshold_5pct_met": und_pct < 5.0,
            "data_date": row["date"],
            "indicator": "SN.ITK.DEFC.ZS",
        }
