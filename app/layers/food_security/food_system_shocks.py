"""Food system shock sensitivity: dual exposure via exports and imports.

Countries with both high food export concentration and high food import
dependency face dual exposure to global food market shocks. Export-side
concentration means revenue collapses when global food prices fall; import
dependence means costs spike when prices rise. Either alone is moderate risk;
both together amplify shock sensitivity.

Methodology:
    food_exports : TX.VAL.FOOD.ZS.UN (food exports as % of merchandise exports)
    food_imports : TM.VAL.FOOD.ZS.UN (food imports as % of merchandise imports)

    export_exposure = clip(food_exports, 0, 100)
    import_exposure = clip(food_imports, 0, 100)

    score = clip(0.5 * export_exposure + 0.5 * import_exposure, 0, 100)

Score (0-100): Higher score = greater dual exposure to global food shocks.

References:
    World Bank (2023). WDI: TX.VAL.FOOD.ZS.UN, TM.VAL.FOOD.ZS.UN.
    Headey, D. & Fan, S. (2010). "Reflections on the global food crisis."
        IFPRI Research Monograph 165.
    FAO (2011). "Price Volatility in Food and Agricultural Markets:
        Policy Responses." FAO Policy Report.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FoodSystemShocks(LayerBase):
    layer_id = "lFS"
    name = "Food System Shocks"

    async def compute(self, db, **kwargs) -> dict:
        """Compute food system shock sensitivity score.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
        """
        country = kwargs.get("country_iso3", "BGD")

        export_row = await db.fetch_one(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.indicator_code = 'TX.VAL.FOOD.ZS.UN'
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )
        if not export_row:
            export_row = await db.fetch_one(
                """
                SELECT dp.value, dp.date
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE '%food%export%%merchandise%'
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country,),
            )

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

        if not export_row and not import_row:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no food export or import share data available",
            }

        food_exports = float(export_row["value"]) if export_row and export_row["value"] is not None else None
        food_imports = float(import_row["value"]) if import_row and import_row["value"] is not None else None

        export_exposure = float(np.clip(food_exports, 0, 100)) if food_exports is not None else 50.0
        import_exposure = float(np.clip(food_imports, 0, 100)) if food_imports is not None else 50.0

        score = float(np.clip(0.5 * export_exposure + 0.5 * import_exposure, 0, 100))

        dual_exposed = (
            food_exports is not None and food_imports is not None
            and food_exports > 30.0 and food_imports > 30.0
        )

        return {
            "score": round(score, 2),
            "country": country,
            "food_exports_pct_merchandise": round(food_exports, 2) if food_exports is not None else None,
            "food_imports_pct_merchandise": round(food_imports, 2) if food_imports is not None else None,
            "component_scores": {
                "export_exposure": round(export_exposure, 2),
                "import_exposure": round(import_exposure, 2),
            },
            "dual_exposed": dual_exposed,
            "data_dates": {
                "food_exports": export_row["date"] if export_row else None,
                "food_imports": import_row["date"] if import_row else None,
            },
            "indicators": ["TX.VAL.FOOD.ZS.UN", "TM.VAL.FOOD.ZS.UN"],
        }
