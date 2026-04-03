"""Child nutrition outcomes: stunting and wasting prevalence.

Stunting (low height-for-age) reflects chronic undernutrition and cumulative
deprivation. Wasting (low weight-for-height) reflects acute malnutrition and
is associated with dramatically elevated child mortality risk. Together they
constitute the most direct anthropometric evidence of food insecurity.

Methodology:
    stunting : SH.STA.STNT.ZS (% of children under 5)
    wasting  : SH.SVR.WAST.ZS (severe wasting, % of children under 5)

    score = clip(stunting * 1.5 + wasting * 2, 0, 100)

Rationale: Wasting is weighted more heavily (factor 2 vs 1.5) because it
reflects acute, life-threatening malnutrition rather than chronic dietary
deficiency. WHO emergency threshold: wasting >15% is a "very high" emergency.

Score (0-100): Higher score = greater child nutrition stress.

References:
    WHO (2010). "Nutrition Landscape Information System Country Profile."
    UNICEF, WHO, World Bank (2021). "Levels and Trends in Child Malnutrition."
    World Bank (2023). WDI: SH.STA.STNT.ZS, SH.SVR.WAST.ZS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class StuntingWasting(LayerBase):
    layer_id = "lFS"
    name = "Stunting and Wasting"

    async def compute(self, db, **kwargs) -> dict:
        """Compute child nutrition stress score from stunting and wasting.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
        """
        country = kwargs.get("country_iso3", "BGD")

        stunting_row = await db.fetch_one(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.indicator_code = 'SH.STA.STNT.ZS'
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )
        if not stunting_row:
            stunting_row = await db.fetch_one(
                """
                SELECT dp.value, dp.date
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE '%stunting%'
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country,),
            )

        wasting_row = await db.fetch_one(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.indicator_code = 'SH.SVR.WAST.ZS'
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )
        if not wasting_row:
            wasting_row = await db.fetch_one(
                """
                SELECT dp.value, dp.date
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE '%wasting%'
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country,),
            )

        if not stunting_row and not wasting_row:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no stunting or wasting data available",
            }

        stunting_pct = float(stunting_row["value"]) if stunting_row and stunting_row["value"] is not None else None
        wasting_pct = float(wasting_row["value"]) if wasting_row and wasting_row["value"] is not None else None

        stunting_component = (stunting_pct * 1.5) if stunting_pct is not None else 0.0
        wasting_component = (wasting_pct * 2.0) if wasting_pct is not None else 0.0

        score = float(np.clip(stunting_component + wasting_component, 0.0, 100.0))

        who_stunting_level = None
        if stunting_pct is not None:
            who_stunting_level = (
                "low" if stunting_pct < 20.0
                else "medium" if stunting_pct < 30.0
                else "high" if stunting_pct < 40.0
                else "very high"
            )

        who_wasting_emergency = wasting_pct is not None and wasting_pct >= 15.0

        return {
            "score": round(score, 2),
            "country": country,
            "stunting_pct": round(stunting_pct, 2) if stunting_pct is not None else None,
            "wasting_pct": round(wasting_pct, 2) if wasting_pct is not None else None,
            "component_scores": {
                "stunting": round(stunting_component, 2),
                "wasting": round(wasting_component, 2),
            },
            "who_stunting_severity": who_stunting_level,
            "who_wasting_emergency": who_wasting_emergency,
            "data_dates": {
                "stunting": stunting_row["date"] if stunting_row else None,
                "wasting": wasting_row["date"] if wasting_row else None,
            },
            "indicators": ["SH.STA.STNT.ZS", "SH.SVR.WAST.ZS"],
        }
