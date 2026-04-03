"""Ocean pollution cost: CO2 emissions and waste intensity as pollution burden proxies.

EN.ATM.CO2E.PC (CO2 per capita) drives ocean acidification, while municipal solid
waste intensity (proxied via urban population share EN.URB.MCTY.TL.ZS) correlates
with plastic leakage into marine systems. UNEP estimates plastic pollution costs
$13B/year globally in ecosystem damage.

Sources: World Bank WDI (EN.ATM.CO2E.PC, EN.URB.MCTY.TL.ZS), UNEP 2014
"""

from __future__ import annotations

from app.layers.base import LayerBase

# UNEP 2014: global annual plastic pollution ecosystem damage
GLOBAL_PLASTIC_DAMAGE_USD = 13e9
# Global mean CO2 per capita (World Bank 2022 estimate)
GLOBAL_MEAN_CO2_PC = 4.7


class OceanPollutionCost(LayerBase):
    layer_id = "lOE"
    name = "Ocean Pollution Cost"

    async def compute(self, db, **kwargs) -> dict:
        co2_code = "EN.ATM.CO2E.PC"
        co2_name = "CO2 emissions per capita"
        co2_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (co2_code, f"%{co2_name}%"),
        )

        urban_code = "EN.URB.MCTY.TL.ZS"
        urban_name = "urban population"
        urban_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (urban_code, f"%{urban_name}%"),
        )

        if not co2_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No CO2 emissions data found",
            }

        co2_vals = [row["value"] for row in co2_rows if row["value"] is not None]
        urban_vals = [row["value"] for row in urban_rows if row["value"] is not None]

        if not co2_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "All CO2 rows have null values",
            }

        co2_latest = float(co2_vals[0])
        urban_latest = float(urban_vals[0]) if urban_vals else 50.0

        # CO2 acidification risk (normalised to global mean)
        co2_ratio = co2_latest / GLOBAL_MEAN_CO2_PC

        # Plastic proxy: higher urbanisation without waste management = more leakage
        plastic_risk = min(urban_latest / 100.0, 1.0)

        # Composite score: 60% CO2 pressure, 40% plastic leakage proxy
        score = round(min(100.0, co2_ratio * 45.0 + plastic_risk * 40.0), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "co2_per_capita": round(co2_latest, 2),
                "global_mean_co2_pc": GLOBAL_MEAN_CO2_PC,
                "co2_ratio_to_global_mean": round(co2_ratio, 3),
                "urban_population_pct": round(urban_latest, 2),
                "plastic_leakage_proxy": round(plastic_risk, 3),
                "global_plastic_damage_usd_bn": round(GLOBAL_PLASTIC_DAMAGE_USD / 1e9, 1),
                "n_co2_obs": len(co2_vals),
                "n_urban_obs": len(urban_vals),
            },
        }
