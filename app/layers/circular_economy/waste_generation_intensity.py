"""Waste generation intensity: waste per capita relative to income proxy.

Uses CO2 emissions per capita (EN.ATM.CO2E.PC) as a proxy for per-capita
material throughput and waste generation intensity. Countries with high
emissions per capita relative to income tend to have high waste intensity.

References:
    World Bank (2018). What a Waste 2.0: A Global Snapshot of Solid Waste Management.
    Kaza, S. et al. (2018). What a Waste 2.0. World Bank.
    World Bank WDI: EN.ATM.CO2E.PC, NY.GDP.PCAP.KD
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class WasteGenerationIntensity(LayerBase):
    layer_id = "lCE"
    name = "Waste Generation Intensity"

    CO2_PC_CODE = "EN.ATM.CO2E.PC"
    GDP_PC_CODE = "NY.GDP.PCAP.KD"

    async def compute(self, db, **kwargs) -> dict:
        co2_pc_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (self.CO2_PC_CODE, f"%{self.CO2_PC_CODE}%"),
        )
        gdp_pc_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (self.GDP_PC_CODE, f"%{self.GDP_PC_CODE}%"),
        )

        if not co2_pc_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no CO2 per capita data for waste generation intensity",
            }

        co2_pc_vals = [r["value"] for r in co2_pc_rows if r["value"] is not None]
        if not co2_pc_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "null CO2 per capita values",
            }

        co2_pc_latest = float(co2_pc_vals[0])
        gdp_pc_latest = None
        if gdp_pc_rows:
            gdp_pc_vals = [r["value"] for r in gdp_pc_rows if r["value"] is not None]
            if gdp_pc_vals:
                gdp_pc_latest = float(gdp_pc_vals[0])

        # Waste intensity: CO2 per capita relative to income (higher = more wasteful per unit income)
        if gdp_pc_latest and gdp_pc_latest > 0:
            waste_intensity_index = co2_pc_latest / (gdp_pc_latest / 10_000.0)
        else:
            waste_intensity_index = co2_pc_latest

        # Trend in CO2 per capita (declining = improving)
        if len(co2_pc_vals) >= 3:
            arr = np.array(co2_pc_vals[:10], dtype=float)
            trend_slope = float(np.polyfit(np.arange(len(arr)), arr, 1)[0])
        else:
            trend_slope = None

        # Score: higher CO2 per capita = higher waste intensity = higher stress
        # Global average ~4.5 tCO2/capita; score rises with intensity
        benchmark_co2_pc = 4.5  # tonnes per capita
        ratio = co2_pc_latest / benchmark_co2_pc
        raw_score = min(ratio * 50.0, 100.0)
        score = float(np.clip(raw_score, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "co2_per_capita_latest_t": round(co2_pc_latest, 3),
            "gdp_per_capita_latest_usd": round(gdp_pc_latest, 0) if gdp_pc_latest else None,
            "waste_intensity_index": round(waste_intensity_index, 4),
            "co2_pc_trend_slope": round(trend_slope, 4) if trend_slope is not None else None,
            "benchmark_co2_pc_t": benchmark_co2_pc,
        }
