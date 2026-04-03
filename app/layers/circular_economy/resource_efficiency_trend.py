"""Resource efficiency trend: declining resource intensity over time.

Computes the trend in carbon intensity (EN.ATM.CO2E.KT / NY.GDP.MKTP.KD)
as a proxy for resource efficiency. A declining ratio means the economy
produces more GDP per unit of resource use (decoupling). The trend slope
and its sign determine the circular economy stress score.

References:
    OECD (2011). Towards Green Growth. OECD Publishing.
    IEA (2023). Energy Efficiency 2023.
    World Bank WDI: EN.ATM.CO2E.KT, NY.GDP.MKTP.KD
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ResourceEfficiencyTrend(LayerBase):
    layer_id = "lCE"
    name = "Resource Efficiency Trend"

    CO2_CODE = "EN.ATM.CO2E.KT"
    GDP_CODE = "NY.GDP.MKTP.KD"

    async def compute(self, db, **kwargs) -> dict:
        co2_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (self.CO2_CODE, f"%{self.CO2_CODE}%"),
        )
        gdp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (self.GDP_CODE, f"%{self.GDP_CODE}%"),
        )

        if not co2_rows or not gdp_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no CO2 or GDP data for resource efficiency trend",
            }

        co2_vals = [r["value"] for r in co2_rows if r["value"] is not None]
        gdp_vals = [r["value"] for r in gdp_rows if r["value"] is not None]

        if not co2_vals or not gdp_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "null values in resource efficiency data",
            }

        n = min(len(co2_vals), len(gdp_vals))
        co2_arr = np.array(co2_vals[:n], dtype=float)
        gdp_arr = np.array(gdp_vals[:n], dtype=float)

        valid = gdp_arr > 0
        if valid.sum() < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient valid observations for trend",
            }

        # Resource intensity: CO2 kt per unit GDP (proxy for resource intensity)
        intensity_arr = co2_arr[valid] / gdp_arr[valid]
        latest_intensity = float(intensity_arr[0])

        # Trend: negative slope = improving efficiency
        if len(intensity_arr) >= 3:
            trend_slope = float(np.polyfit(np.arange(len(intensity_arr)), intensity_arr, 1)[0])
        else:
            trend_slope = None

        # Percent change from oldest to newest in sample
        if len(intensity_arr) >= 2:
            pct_change = (intensity_arr[0] - intensity_arr[-1]) / intensity_arr[-1] * 100.0
        else:
            pct_change = None

        # Score: improving efficiency = low stress; worsening = high stress
        if trend_slope is not None:
            if trend_slope < -1e-10:
                # Improving: map slope magnitude to score (0-40)
                raw_score = max(0.0, 40.0 + trend_slope * 1e8)
            else:
                # Worsening: higher slope = more stress
                raw_score = min(40.0 + trend_slope * 1e8, 100.0)
        else:
            raw_score = 50.0

        score = float(np.clip(raw_score, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "resource_intensity_latest": round(latest_intensity, 8),
            "resource_intensity_trend_slope": round(trend_slope, 10) if trend_slope is not None else None,
            "efficiency_change_pct": round(pct_change, 2) if pct_change is not None else None,
            "trend_direction": "improving" if (trend_slope is not None and trend_slope < 0) else "worsening",
            "n_observations": int(valid.sum()),
        }
