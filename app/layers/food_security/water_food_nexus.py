"""Water-food nexus risk: water stress amplified by agricultural dependence.

When a country relies heavily on agriculture for economic output and that
agriculture depends on increasingly scarce freshwater, the food-water nexus
creates compounding vulnerability. Both dimensions must be elevated for peak
risk: high water stress alone may not affect food security if agriculture is
a small share of GDP, and vice versa.

Methodology:
    water_stress : ER.H2O.FWTL.ZS (freshwater withdrawals as % of internal
                   renewable freshwater resources)
    ag_share     : NV.AGR.TOTL.ZS (agriculture, value added as % of GDP)

    water_stress_score = clip(water_stress, 0, 100)
    ag_dependence_score = clip(ag_share * 2, 0, 100)
        (50% ag share = maximum dependence score)

    nexus_score = clip(sqrt(water_stress_score * ag_dependence_score), 0, 100)
        (geometric mean captures interaction: both must be elevated for high risk)

Score (0-100): Higher score = greater water-food nexus risk.

References:
    Falkenmark, M. & Rockstrom, J. (2004). "Balancing Water for Humans
        and Nature." Earthscan.
    World Bank (2023). WDI: ER.H2O.FWTL.ZS, NV.AGR.TOTL.ZS.
    FAO (2014). "The Water-Energy-Food Nexus: A New Approach in Support
        of Food Security and Sustainable Agriculture."
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class WaterFoodNexus(LayerBase):
    layer_id = "lFS"
    name = "Water-Food Nexus"

    async def compute(self, db, **kwargs) -> dict:
        """Compute water-food nexus risk score.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
        """
        country = kwargs.get("country_iso3", "BGD")

        water_row = await db.fetch_one(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.indicator_code = 'ER.H2O.FWTL.ZS'
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )
        if not water_row:
            water_row = await db.fetch_one(
                """
                SELECT dp.value, dp.date
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE '%freshwater%withdrawal%'
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country,),
            )

        ag_row = await db.fetch_one(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.indicator_code = 'NV.AGR.TOTL.ZS'
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )
        if not ag_row:
            ag_row = await db.fetch_one(
                """
                SELECT dp.value, dp.date
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE '%agriculture%value%added%%GDP%'
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country,),
            )

        if not water_row and not ag_row:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no water stress or agricultural share data available",
            }

        water_stress = float(water_row["value"]) if water_row and water_row["value"] is not None else None
        ag_share = float(ag_row["value"]) if ag_row and ag_row["value"] is not None else None

        water_stress_score = float(np.clip(water_stress, 0, 100)) if water_stress is not None else 50.0
        ag_dependence_score = float(np.clip(ag_share * 2.0, 0, 100)) if ag_share is not None else 50.0

        nexus_score = float(np.clip(np.sqrt(water_stress_score * ag_dependence_score), 0, 100))

        return {
            "score": round(nexus_score, 2),
            "country": country,
            "water_withdrawal_pct_resources": round(water_stress, 2) if water_stress is not None else None,
            "ag_value_added_pct_gdp": round(ag_share, 2) if ag_share is not None else None,
            "component_scores": {
                "water_stress_score": round(water_stress_score, 2),
                "ag_dependence_score": round(ag_dependence_score, 2),
            },
            "interaction_method": "geometric_mean",
            "data_dates": {
                "water_stress": water_row["date"] if water_row else None,
                "ag_share": ag_row["date"] if ag_row else None,
            },
            "indicators": ["ER.H2O.FWTL.ZS", "NV.AGR.TOTL.ZS"],
        }
