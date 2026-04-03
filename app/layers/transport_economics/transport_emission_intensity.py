"""Transport Emission Intensity module.

Estimates transport-related emission intensity by combining per-capita CO2
emissions with air passenger volume. High emissions relative to air travel
volume signals an emission-inefficient transport system.

Indicators: EN.ATM.CO2E.PC (CO2 emissions per capita, metric tons),
            IS.AIR.PSGR (air passengers carried).
Emission intensity ratio = CO2_pc / log10(passengers + 1).
Score = clip(ratio / 5 * 100, 0, 100). Higher = more emission-intensive.

Sources: WDI EN.ATM.CO2E.PC, IS.AIR.PSGR
"""

from __future__ import annotations

import math

import numpy as np

from app.layers.base import LayerBase

_RATIO_CEILING = 5.0  # co2_pc / log10(passengers) ceiling for normalization


class TransportEmissionIntensity(LayerBase):
    layer_id = "lTR"
    name = "Transport Emission Intensity"

    async def compute(self, db, **kwargs) -> dict:
        co2_code = "EN.ATM.CO2E.PC"
        psgr_code = "IS.AIR.PSGR"

        co2_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (co2_code, f"%{co2_code}%"),
        )
        psgr_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (psgr_code, f"%{psgr_code}%"),
        )

        if not co2_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for EN.ATM.CO2E.PC"}

        co2_pc = float(co2_rows[0]["value"])
        metrics: dict = {"co2_per_capita_mt": round(co2_pc, 3)}

        if psgr_rows:
            passengers = float(psgr_rows[0]["value"])
            log_psgr = math.log10(passengers + 1) if passengers > 0 else 1.0
            ratio = co2_pc / log_psgr if log_psgr > 0 else co2_pc
            score = float(np.clip(ratio / _RATIO_CEILING * 100.0, 0, 100))
            metrics["air_passengers"] = int(passengers)
            metrics["log10_passengers"] = round(log_psgr, 3)
            metrics["emission_intensity_ratio"] = round(ratio, 4)
        else:
            # Fallback: use co2_pc alone (threshold: 15 MT = high intensity)
            score = float(np.clip(co2_pc / 15.0 * 100.0, 0, 100))

        return {
            "score": round(score, 1),
            "signal": self.classify_signal(score),
            "metrics": metrics,
            "_sources": ["WDI:EN.ATM.CO2E.PC", "WDI:IS.AIR.PSGR"],
        }
