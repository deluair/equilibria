"""Food import dependency: food imports as share of merchandise imports.

Measures a country's reliance on food imports relative to total merchandise
imports (WDI indicator TM.VAL.FOOD.ZS.UN). A high share signals food security
vulnerability because it exposes the country to international price shocks,
exchange rate risk, and supply chain disruptions.

Methodology:
    Fetch the latest available value of TM.VAL.FOOD.ZS.UN (food imports as %
    of merchandise imports). The stress score is:

        score = clip(food_import_share * 1.2, 0, 100)

    At 0%: score = 0 (no vulnerability).
    At 83%: score = 100 (maximum stress; typical for highly food-import-dependent
    low-income countries).
    At 20%: score = 24 (moderate vulnerability, near world average).

Score (0-100): Higher score indicates greater food import dependency and
associated food security vulnerability.

References:
    World Bank WDI indicator TM.VAL.FOOD.ZS.UN.
    FAO (2021). "The State of Food Security and Nutrition in the World."
    Clapp, J. (2015). "Food security and international trade." FAO.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FoodImportDependency(LayerBase):
    layer_id = "l5"
    name = "Food Import Dependency"

    async def compute(self, db, **kwargs) -> dict:
        """Compute food import dependency stress score.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
        """
        country = kwargs.get("country_iso3", "BGD")

        # Primary: indicator code lookup
        row = await db.fetch_one(
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

        if not row:
            # Fallback: name-based lookup
            row = await db.fetch_one(
                """
                SELECT dp.value, dp.date
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND (ds.name LIKE '%food%import%' AND ds.name LIKE '%merchandise%')
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country,),
            )

        if not row or row["value"] is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "food import share data unavailable (TM.VAL.FOOD.ZS.UN)",
            }

        food_import_share = float(row["value"])
        latest_date = row["date"]

        # Fetch historical series for trend
        history = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.indicator_code = 'TM.VAL.FOOD.ZS.UN'
                   OR (ds.name LIKE '%food%import%' AND ds.name LIKE '%merchandise%'))
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        trend_slope = None
        if len(history) >= 5:
            from scipy.stats import linregress
            years = []
            vals = []
            for r in history:
                if r["value"] is not None:
                    try:
                        years.append(int(str(r["date"])[:4]))
                        vals.append(float(r["value"]))
                    except (ValueError, TypeError):
                        continue
            if len(years) >= 5:
                res = linregress(np.array(years, dtype=float), np.array(vals, dtype=float))
                trend_slope = round(float(res.slope), 4)

        score = float(np.clip(food_import_share * 1.2, 0.0, 100.0))

        vulnerability_level = (
            "critical" if food_import_share > 60
            else "high" if food_import_share > 40
            else "moderate" if food_import_share > 20
            else "low"
        )

        return {
            "score": round(score, 2),
            "country": country,
            "food_import_share_pct": round(food_import_share, 2),
            "latest_date": latest_date,
            "vulnerability_level": vulnerability_level,
            "trend_slope_pp_per_year": trend_slope,
            "indicator": "TM.VAL.FOOD.ZS.UN",
            "n_historical_obs": len(history),
        }
