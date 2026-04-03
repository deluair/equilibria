"""Blue carbon value: coastal ecosystem valuation via forest cover and CO2 proxy.

Mangroves, seagrasses, and salt marshes sequester carbon at 3-5x the rate of
terrestrial forests. Forest cover (AG.LND.FRST.ZS) proxies coastal vegetation,
while CO2 emissions per capita (EN.ATM.CO2E.PC) estimates the sink offset value.

Sources: World Bank WDI (AG.LND.FRST.ZS, EN.ATM.CO2E.PC), IPCC AR6
"""

from __future__ import annotations

from app.layers.base import LayerBase

# Blue carbon sequestration multiplier vs terrestrial forest (IPCC estimate)
BLUE_CARBON_MULTIPLIER = 4.0
# Social cost of carbon proxy ($/tCO2, US EPA 2023 central estimate)
SOCIAL_COST_CARBON = 190.0


class BlueCarbonValue(LayerBase):
    layer_id = "lOE"
    name = "Blue Carbon Value"

    async def compute(self, db, **kwargs) -> dict:
        forest_code = "AG.LND.FRST.ZS"
        forest_name = "forest area"
        forest_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (forest_code, f"%{forest_name}%"),
        )

        co2_code = "EN.ATM.CO2E.PC"
        co2_name = "CO2 emissions per capita"
        co2_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (co2_code, f"%{co2_name}%"),
        )

        if not forest_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No forest cover data found",
            }

        forest_vals = [row["value"] for row in forest_rows if row["value"] is not None]
        co2_vals = [row["value"] for row in co2_rows if row["value"] is not None]

        if not forest_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "All forest cover rows have null values",
            }

        forest_latest = float(forest_vals[0])
        co2_latest = float(co2_vals[0]) if co2_vals else None

        # Coastal blue carbon area approximated as 5-10% of forest cover
        blue_carbon_pct = forest_latest * 0.07

        # Trend in forest cover (proxy for ecosystem health)
        trend = "stable"
        if len(forest_vals) >= 3:
            recent = sum(forest_vals[:3]) / 3
            older = sum(forest_vals[-3:]) / 3
            if recent < older * 0.97:
                trend = "declining"
            elif recent > older * 1.03:
                trend = "growing"

        # Score: low/declining forest + high CO2 = high risk to blue carbon stores
        forest_risk = max(0.0, 1.0 - forest_latest / 60.0)  # 60% forest = low risk
        co2_risk = min(co2_latest / 20.0, 1.0) if co2_latest is not None else 0.5
        trend_penalty = 15.0 if trend == "declining" else 0.0

        score = round(min(100.0, forest_risk * 50.0 + co2_risk * 35.0 + trend_penalty), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "forest_cover_pct": round(forest_latest, 2),
                "blue_carbon_area_pct_est": round(blue_carbon_pct, 3),
                "co2_per_capita": round(co2_latest, 2) if co2_latest is not None else None,
                "social_cost_carbon_usd_per_t": SOCIAL_COST_CARBON,
                "blue_carbon_multiplier": BLUE_CARBON_MULTIPLIER,
                "forest_trend": trend,
                "n_forest_obs": len(forest_vals),
            },
        }
