"""Ecological overshoot: CO2 emissions per capita vs biocapacity proxy.

Compares per-capita CO2 emissions (EN.ATM.CO2E.PC) against a biocapacity proxy
derived from forest area per capita (AG.LND.FRST.K2 / SP.POP.TOTL). Countries
emitting far beyond what their biocapacity can absorb are in ecological overshoot.

Overshoot ratio = CO2 per capita / (forest area per capita * absorption factor)
where absorption factor ~ 0.63 tCO2/ha/year (IPCC average for global forests).

Score: ratio <= 1 (within biocapacity) -> 20, ratio >= 10 -> 90.

References:
    Global Footprint Network. "Ecological Footprint." footprintnetwork.org.
    Pan, Y. et al. (2011). "A large and persistent carbon sink in the world's
        forests." Science, 333(6045), 988-993. (0.63 tCO2/ha/yr estimate)
    World Bank WDI: EN.ATM.CO2E.PC, AG.LND.FRST.K2, SP.POP.TOTL.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# IPCC average net forest CO2 absorption (tCO2/ha/year, tropical+temperate blend)
FOREST_ABSORPTION_TCO2_PER_HA = 0.63


class EcologicalOvershoot(LayerBase):
    layer_id = "lEA"
    name = "Ecological Overshoot"

    async def compute(self, db, **kwargs) -> dict:
        # CO2 per capita (metric tons)
        co2_code = "EN.ATM.CO2E.PC"
        co2_name = "CO2 emissions per capita"
        co2_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (co2_code, f"%{co2_name}%"),
        )

        # Forest area (sq km)
        forest_code = "AG.LND.FRST.K2"
        forest_name = "forest area"
        forest_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (forest_code, f"%{forest_name}%"),
        )

        # Population
        pop_code = "SP.POP.TOTL"
        pop_name = "total population"
        pop_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (pop_code, f"%{pop_name}%"),
        )

        co2_vals = [float(r["value"]) for r in co2_rows if r["value"] is not None]
        forest_vals = [float(r["value"]) for r in forest_rows if r["value"] is not None]
        pop_vals = [float(r["value"]) for r in pop_rows if r["value"] is not None]

        if not co2_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no CO2 per capita data"}

        co2_pc = co2_vals[0]  # tCO2 per person

        overshoot_ratio = None
        biocapacity_tco2_pc = None

        if forest_vals and pop_vals and pop_vals[0] > 0:
            forest_ha = forest_vals[0] * 100.0  # sq km -> ha (1 sq km = 100 ha)
            pop = pop_vals[0]
            forest_ha_pc = forest_ha / pop  # ha per person
            biocapacity_tco2_pc = forest_ha_pc * FOREST_ABSORPTION_TCO2_PER_HA
            if biocapacity_tco2_pc > 0:
                overshoot_ratio = co2_pc / biocapacity_tco2_pc

        if overshoot_ratio is None:
            # Fallback: use global average biocapacity (~1.6 gha/person ~ 1.0 tCO2 absorb)
            overshoot_ratio = co2_pc / 1.0

        # Score: ratio 1 -> 30, ratio 10 -> 90
        score = float(np.clip(20.0 + np.log1p(max(overshoot_ratio - 1.0, 0.0)) * 12.0, 10.0, 95.0))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "co2_per_capita_tco2": round(co2_pc, 2),
                "biocapacity_tco2_per_capita": round(biocapacity_tco2_pc, 4) if biocapacity_tco2_pc is not None else None,
                "overshoot_ratio": round(overshoot_ratio, 2),
                "in_overshoot": overshoot_ratio > 1.0,
                "forest_absorption_factor": FOREST_ABSORPTION_TCO2_PER_HA,
            },
        }
