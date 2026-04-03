"""Material productivity: GDP per unit of material consumption proxy.

Uses GDP (constant USD) relative to CO2 emissions (kt) as a proxy for
material productivity — higher GDP output per unit of resource-intensive
activity signals a more productive, less wasteful economy.

References:
    Eurostat (2023). Material Flow Accounts and Resource Productivity.
    OECD (2019). Global Material Resources Outlook to 2060.
    World Bank WDI: NY.GDP.MKTP.KD, EN.ATM.CO2E.KT
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MaterialProductivity(LayerBase):
    layer_id = "lCE"
    name = "Material Productivity"

    GDP_CODE = "NY.GDP.MKTP.KD"
    CO2_CODE = "EN.ATM.CO2E.KT"

    async def compute(self, db, **kwargs) -> dict:
        gdp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (self.GDP_CODE, f"%{self.GDP_CODE}%"),
        )
        co2_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (self.CO2_CODE, f"%{self.CO2_CODE}%"),
        )

        if not gdp_rows or not co2_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no GDP or CO2 data for material productivity",
            }

        gdp_vals = [r["value"] for r in gdp_rows if r["value"] is not None]
        co2_vals = [r["value"] for r in co2_rows if r["value"] is not None]

        if not gdp_vals or not co2_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "null values in material productivity data",
            }

        gdp_latest = float(gdp_vals[0])
        co2_latest = float(co2_vals[0])

        if co2_latest <= 0:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "zero or negative CO2 value",
            }

        # Material productivity: GDP per kt CO2 (proxy for GDP per material unit)
        mat_productivity = gdp_latest / co2_latest  # USD per kt CO2

        # Trend: improving productivity (ratio rising) = lower stress
        n = min(len(gdp_vals), len(co2_vals), 10)
        gdp_arr = np.array(gdp_vals[:n], dtype=float)
        co2_arr = np.array(co2_vals[:n], dtype=float)
        valid = co2_arr > 0
        if valid.sum() >= 3:
            ratio_arr = gdp_arr[valid] / co2_arr[valid]
            trend_slope = float(np.polyfit(np.arange(valid.sum()), ratio_arr, 1)[0])
        else:
            trend_slope = None

        # Score: higher productivity = lower stress (0 = best economy, 100 = worst)
        # Benchmark: WB global average ~$1M GDP per kt CO2 for upper-middle-income
        benchmark = 1_000_000.0
        ratio_to_benchmark = mat_productivity / benchmark
        # Score inverted: below benchmark = higher stress
        raw_score = max(0.0, (1.0 - ratio_to_benchmark) * 100.0)
        score = float(np.clip(raw_score, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "material_productivity_usd_per_kt_co2": round(mat_productivity, 2),
            "gdp_latest_usd": round(gdp_latest, 0),
            "co2_latest_kt": round(co2_latest, 0),
            "productivity_trend_slope": round(trend_slope, 4) if trend_slope is not None else None,
            "benchmark_usd_per_kt_co2": benchmark,
        }
