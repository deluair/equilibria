"""Marine bioeconomy: aquaculture and marine biotech value as share of ocean economy.

The marine bioeconomy encompasses aquaculture, marine fisheries, marine biotechnology
(bio-active compounds from marine organisms), marine pharmaceuticals, and seaweed-based
industries. It represents a fast-growing segment of the blue economy with high
value-added potential when processed domestically rather than exported raw.

Score: high fish exports with significant aquaculture relative to wild catch -> STABLE
(managed marine bioeconomy), high wild fishery dependence with declining stocks -> STRESS
(unsustainable extraction eroding marine biological capital).

Proxies: fish exports (% merchandise exports) as marine bioeconomy size signal,
food trade balance and agricultural value added for context.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class MarineBioeconomy(LayerBase):
    layer_id = "lBI"
    name = "Marine Bioeconomy"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        fish_exp_code = "TX.VAL.FISH.ZS.UN"
        food_exp_code = "TX.VAL.FOOD.ZS.UN"
        agri_code = "NV.AGR.TOTL.ZS"

        fish_exp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (fish_exp_code, "%Fish exports%"),
        )
        food_exp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (food_exp_code, "%Food exports%"),
        )
        agri_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (agri_code, "%Agriculture, forestry%"),
        )

        fish_exp_vals = [r["value"] for r in fish_exp_rows if r["value"] is not None]
        food_exp_vals = [r["value"] for r in food_exp_rows if r["value"] is not None]
        agri_vals = [r["value"] for r in agri_rows if r["value"] is not None]

        # Fish export data preferred; fall back to food exports as proxy
        if not fish_exp_vals and not food_exp_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for fish or food exports (TX.VAL.FISH.ZS.UN / TX.VAL.FOOD.ZS.UN)",
            }

        fish_pct = fish_exp_vals[0] if fish_exp_vals else None
        food_pct = food_exp_vals[0] if food_exp_vals else None
        agri_gdp = agri_vals[0] if agri_vals else None

        # Construct marine bioeconomy intensity
        # Primary signal: fish export share of merchandise exports
        if fish_pct is not None:
            marine_intensity = fish_pct
        else:
            # Use food exports as broader proxy with discount
            marine_intensity = (food_pct or 0) * 0.2

        # Score: moderate marine intensity with growing sector = good
        # Very low = marginal marine economy, very high = raw-export dependence
        if marine_intensity >= 15:
            base = 30.0 + (marine_intensity - 15) * 1.0  # raw-export risk
        elif marine_intensity >= 5:
            base = 15.0 + (marine_intensity - 5) * 1.5
        elif marine_intensity >= 1:
            base = 30.0 + (5 - marine_intensity) * 3.0
        else:
            base = min(70.0, 42.0 + (1 - marine_intensity) * 10.0)

        # Agriculture GDP share: high agri with low fish = inland-oriented (mild penalty)
        if agri_gdp is not None and agri_gdp > 20 and marine_intensity < 2:
            base = min(100.0, base + 8.0)

        score = round(min(100.0, base), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "fish_exports_pct": round(fish_pct, 2) if fish_pct is not None else None,
                "food_exports_pct": round(food_pct, 2) if food_pct is not None else None,
                "marine_intensity_index": round(marine_intensity, 3),
                "agriculture_value_added_gdp_pct": round(agri_gdp, 2) if agri_gdp is not None else None,
                "n_obs_fish": len(fish_exp_vals),
                "n_obs_food": len(food_exp_vals),
            },
        }
