"""Forest bioeconomy value: sustainably managed forest value and non-timber exports.

Forests are foundational bioeconomy assets: timber, pulp, non-timber forest products
(NTFPs such as resins, medicinal plants, wild foods), carbon sequestration services,
and ecotourism. A well-managed forest bioeconomy captures value across all layers
while maintaining the biological capital stock.

Score: high forest cover + expanding agricultural land + high food/agri exports
-> WATCH (forest conversion pressure), high forest area with declining deforestation
and rising NTFP-proxied export value -> STABLE (sustainable extraction).

Proxies: forest area (% of land area), food exports (% merchandise exports) as
NTFP/agri-export proxy, agricultural land change as deforestation pressure signal.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class ForestBioeconomyValue(LayerBase):
    layer_id = "lBI"
    name = "Forest Bioeconomy Value"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        forest_code = "AG.LND.FRST.ZS"
        food_exp_code = "TX.VAL.FOOD.ZS.UN"
        agri_land_code = "AG.LND.AGRI.ZS"

        forest_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (forest_code, "%Forest area%"),
        )
        food_exp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (food_exp_code, "%Food exports%"),
        )
        agri_land_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (agri_land_code, "%Agricultural land%"),
        )

        forest_vals = [r["value"] for r in forest_rows if r["value"] is not None]
        food_exp_vals = [r["value"] for r in food_exp_rows if r["value"] is not None]
        agri_land_vals = [r["value"] for r in agri_land_rows if r["value"] is not None]

        if not forest_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for forest area AG.LND.FRST.ZS",
            }

        forest_pct = forest_vals[0]
        food_exp_pct = food_exp_vals[0] if food_exp_vals else None

        # Agricultural land trend as deforestation pressure indicator
        agri_land_trend = None
        if len(agri_land_vals) > 1:
            agri_land_trend = round(agri_land_vals[0] - agri_land_vals[-1], 3)

        # Base score from forest cover: higher = more forest bioeconomy potential
        # Low forest cover = depleted bioeconomy asset base
        if forest_pct >= 50:
            base = 12.0
        elif forest_pct >= 30:
            base = 12.0 + (50 - forest_pct) * 0.9
        elif forest_pct >= 15:
            base = 30.0 + (30 - forest_pct) * 1.5
        elif forest_pct >= 5:
            base = 52.5 + (15 - forest_pct) * 2.0
        else:
            base = min(90.0, 72.5 + (5 - forest_pct) * 3.0)

        # Expanding agricultural land signals forest conversion (raises stress)
        if agri_land_trend is not None:
            if agri_land_trend > 2.0:
                base = min(100.0, base + 12.0)
            elif agri_land_trend > 0.5:
                base = min(100.0, base + 5.0)
            elif agri_land_trend < -0.5:
                base = max(5.0, base - 5.0)  # reforestation signal

        # Food/agri export share as NTFP-export proxy: moderate is good
        if food_exp_pct is not None:
            if 10 <= food_exp_pct <= 40:
                base = max(5.0, base - 5.0)  # healthy agri export base
            elif food_exp_pct > 60:
                base = min(100.0, base + 5.0)  # over-reliance on raw exports

        score = round(min(100.0, base), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "forest_area_pct": round(forest_pct, 2),
                "food_exports_pct": round(food_exp_pct, 2) if food_exp_pct is not None else None,
                "agricultural_land_trend_pct": agri_land_trend,
                "n_obs_forest": len(forest_vals),
                "n_obs_food_exp": len(food_exp_vals),
                "n_obs_agri_land": len(agri_land_vals),
                "deforestation_pressure": agri_land_trend is not None and agri_land_trend > 1.0,
            },
        }
