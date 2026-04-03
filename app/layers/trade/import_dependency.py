"""Import dependency: critical import vulnerability via food and fuel shares.

Countries highly dependent on imported food and fuel face acute exposure to
commodity price shocks, supply disruptions, and exchange rate movements.
High combined food + fuel import share of total merchandise imports signals
structural import vulnerability.

Indicators:
- TM.VAL.FOOD.ZS.UN: Food imports as % of merchandise imports
- TM.VAL.FUEL.ZS.UN: Fuel imports as % of merchandise imports

Combined dependency score:
    combined_share = food_share + fuel_share (can exceed 100% if both high)

Score: combined_share mapped linearly. 0% = no stress; 80%+ = severe stress.
Trend penalty for worsening dependency.
"""

import numpy as np

from app.layers.base import LayerBase


class ImportDependency(LayerBase):
    layer_id = "l1"
    name = "Import Dependency"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.wdi_code
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.wdi_code IN ('TM.VAL.FOOD.ZS.UN', 'TM.VAL.FUEL.ZS.UN')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no food/fuel import share data"}

        food: dict[str, float] = {}
        fuel: dict[str, float] = {}

        for row in rows:
            val = row["value"]
            if val is None:
                continue
            code = row["wdi_code"]
            date = row["date"]
            if code == "TM.VAL.FOOD.ZS.UN":
                food[date] = float(val)
            elif code == "TM.VAL.FUEL.ZS.UN":
                fuel[date] = float(val)

        if not food and not fuel:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all import dependency data rows are null"}

        # Build combined series on dates where at least one indicator exists
        all_dates = sorted(set(food.keys()) | set(fuel.keys()))
        combined = []
        food_vals = []
        fuel_vals = []
        for d in all_dates:
            f = food.get(d)
            u = fuel.get(d)
            if f is None and u is None:
                continue
            fv = f if f is not None else 0.0
            uv = u if u is not None else 0.0
            food_vals.append(fv)
            fuel_vals.append(uv)
            combined.append(fv + uv)

        if not combined:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid import dependency observations"}

        arr = np.array(combined)
        latest_combined = float(arr[-1])
        mean_combined = float(np.mean(arr))
        latest_food = float(food_vals[-1]) if food_vals else None
        latest_fuel = float(fuel_vals[-1]) if fuel_vals else None

        # OLS trend on combined share
        t = np.arange(len(arr), dtype=float)
        X = np.column_stack([np.ones(len(t)), t])
        beta = np.linalg.lstsq(X, arr, rcond=None)[0]
        trend_slope = float(beta[1])

        # Score: combined share 0-80+ -> 0-80 level score
        level_score = float(np.clip(latest_combined * 0.875, 0.0, 70.0))

        # Trend: worsening (rising) dependency adds up to 30 pts
        trend_score = max(0.0, min(30.0, trend_slope * 3.0)) if trend_slope > 0 else 0.0

        score = float(np.clip(level_score + trend_score, 0.0, 100.0))

        result = {
            "score": round(score, 2),
            "country": country,
            "latest_combined_share_pct": round(latest_combined, 4),
            "mean_combined_share_pct": round(mean_combined, 4),
            "trend_slope_pct_per_year": round(trend_slope, 6),
            "trend_direction": "worsening" if trend_slope > 0 else "improving",
            "n_observations": len(combined),
            "date_range": [all_dates[0], all_dates[-1]],
        }
        if latest_food is not None:
            result["latest_food_import_share_pct"] = round(latest_food, 4)
        if latest_fuel is not None:
            result["latest_fuel_import_share_pct"] = round(latest_fuel, 4)

        return result
