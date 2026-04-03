"""Food price inflation stress from import share and consumer price inflation.

High food import dependency amplifies general consumer price inflation into
food price stress. Countries that rely heavily on imported food are exposed to
international price movements and exchange rate shocks.

Methodology:
    food_import_share : TM.VAL.FOOD.ZS.UN (% of merchandise imports)
    cpi_inflation     : FP.CPI.TOTL.ZG (annual % CPI change)

    food_import_share is scaled 0-100 (direct percentage).
    cpi_inflation: moderate inflation (<=5%) = low stress; >30% = crisis.

    import_stress = clip(food_import_share, 0, 100)
    inflation_stress = clip(max(0, cpi - 5) / 25 * 100, 0, 100)
    score = clip(0.4 * import_stress + 0.6 * inflation_stress, 0, 100)

Rationale: Inflation is the proximate stressor; high food import share is the
amplifier that translates that inflation into food price insecurity.

Score (0-100): Higher score = greater food price inflation stress.

References:
    World Bank (2023). WDI: TM.VAL.FOOD.ZS.UN, FP.CPI.TOTL.ZG.
    FAO (2011). "The State of Food and Agriculture 2011."
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FoodPriceInflation(LayerBase):
    layer_id = "lFS"
    name = "Food Price Inflation"

    async def compute(self, db, **kwargs) -> dict:
        """Compute food price inflation stress score.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
        """
        country = kwargs.get("country_iso3", "BGD")

        import_row = await db.fetch_one(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.indicator_code = 'TM.VAL.FOOD.ZS.UN'
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )
        if not import_row:
            import_row = await db.fetch_one(
                """
                SELECT dp.value, dp.date
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE '%food%import%%merchandise%'
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country,),
            )

        cpi_row = await db.fetch_one(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.indicator_code = 'FP.CPI.TOTL.ZG'
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )
        if not cpi_row:
            cpi_row = await db.fetch_one(
                """
                SELECT dp.value, dp.date
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE '%consumer%price%inflation%'
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country,),
            )

        if not import_row and not cpi_row:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no food import share or CPI data available",
            }

        food_import_share = float(import_row["value"]) if import_row and import_row["value"] is not None else None
        cpi_inflation = float(cpi_row["value"]) if cpi_row and cpi_row["value"] is not None else None

        import_stress = float(np.clip(food_import_share, 0, 100)) if food_import_share is not None else 50.0
        inflation_stress = (
            float(np.clip(max(0.0, cpi_inflation - 5.0) / 25.0 * 100.0, 0, 100))
            if cpi_inflation is not None
            else 50.0
        )

        score = float(np.clip(0.4 * import_stress + 0.6 * inflation_stress, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "food_import_share_pct": round(food_import_share, 2) if food_import_share is not None else None,
            "cpi_inflation_pct": round(cpi_inflation, 2) if cpi_inflation is not None else None,
            "component_scores": {
                "import_stress": round(import_stress, 2),
                "inflation_stress": round(inflation_stress, 2),
            },
            "data_dates": {
                "food_import_share": import_row["date"] if import_row else None,
                "cpi_inflation": cpi_row["date"] if cpi_row else None,
            },
            "indicators": ["TM.VAL.FOOD.ZS.UN", "FP.CPI.TOTL.ZG"],
        }
