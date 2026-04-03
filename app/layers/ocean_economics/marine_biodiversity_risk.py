"""Marine biodiversity risk: fish stock depletion and ocean temperature pressure.

Fish stock depletion is proxied via agriculture value added trend (NV.AGR.TOTL.ZS)
as capture fisheries compose a significant fraction of agrarian GDP in coastal nations.
Ocean temperature pressure is proxied by CO2 per capita (EN.ATM.CO2E.PC), which
drives thermal stress on marine ecosystems. FAO estimates 35.4% of stocks are
fished at biologically unsustainable levels (State of World Fisheries 2022).

Sources: World Bank WDI (NV.AGR.TOTL.ZS, EN.ATM.CO2E.PC), FAO 2022
"""

from __future__ import annotations

from app.layers.base import LayerBase

# FAO (2022): fraction of global stocks at biologically unsustainable levels
FAO_GLOBAL_UNSUSTAINABLE_FRACTION = 0.354


class MarineBiodiversityRisk(LayerBase):
    layer_id = "lOE"
    name = "Marine Biodiversity Risk"

    async def compute(self, db, **kwargs) -> dict:
        ag_code = "NV.AGR.TOTL.ZS"
        ag_name = "agriculture value added"
        ag_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (ag_code, f"%{ag_name}%"),
        )

        co2_code = "EN.ATM.CO2E.PC"
        co2_name = "CO2 emissions per capita"
        co2_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (co2_code, f"%{co2_name}%"),
        )

        if not ag_rows and not co2_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No marine biodiversity risk data found",
            }

        ag_vals = [row["value"] for row in ag_rows if row["value"] is not None]
        co2_vals = [row["value"] for row in co2_rows if row["value"] is not None]

        ag_latest = float(ag_vals[0]) if ag_vals else None
        co2_latest = float(co2_vals[0]) if co2_vals else None

        if ag_latest is None and co2_latest is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "All marine biodiversity rows have null values",
            }

        # Fish stock depletion proxy: lower/declining agriculture share
        # suggests fisheries are shrinking relative to economy
        depletion_score = 0.0
        trend = "stable"
        if ag_vals and len(ag_vals) >= 3:
            recent = sum(ag_vals[:3]) / 3
            older = sum(ag_vals[-3:]) / 3
            if recent < older * 0.95:
                trend = "declining"
                depletion_score = 35.0
            elif recent < older * 0.99:
                trend = "slightly_declining"
                depletion_score = 18.0
        elif ag_latest is not None:
            depletion_score = 20.0  # unknown trend, moderate baseline

        # Ocean temperature proxy via CO2 pressure
        if co2_latest is not None:
            # >10 tCO2/cap = high thermal stress risk
            thermal_score = min(co2_latest / 10.0 * 50.0, 50.0)
        else:
            thermal_score = 25.0

        # FAO baseline global risk adjustment
        fao_baseline = FAO_GLOBAL_UNSUSTAINABLE_FRACTION * 15.0

        score = round(min(100.0, depletion_score + thermal_score + fao_baseline), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "agriculture_pct_gdp": round(ag_latest, 2) if ag_latest is not None else None,
                "ag_trend": trend,
                "co2_per_capita": round(co2_latest, 2) if co2_latest is not None else None,
                "depletion_component": round(depletion_score, 2),
                "thermal_pressure_component": round(thermal_score, 2),
                "fao_global_unsustainable_pct": round(FAO_GLOBAL_UNSUSTAINABLE_FRACTION * 100, 1),
                "n_ag_obs": len(ag_vals),
                "n_co2_obs": len(co2_vals),
            },
        }
