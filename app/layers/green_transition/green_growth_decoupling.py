"""Green growth decoupling: NY.GDP.MKTP.KD.ZG vs EN.ATM.CO2E.KT.

Methodology
-----------
Green growth decoupling measures whether a country can grow its economy while
reducing absolute CO2 emissions. This is the core test of whether economic
development and environmental impact have been decoupled.

Three decoupling states are distinguished:
1. Absolute decoupling: GDP grows AND CO2 falls (ideal)
2. Relative decoupling: both grow but GDP faster than CO2 (insufficient but better)
3. No decoupling: CO2 grows as fast or faster than GDP (worst)

Formally:
    gdp_growth_rate = slope / mean of NY.GDP.MKTP.KD.ZG (direct % values)
    co2_growth_rate = slope of log(EN.ATM.CO2E.KT) * 100

    decoupling_index = gdp_growth_rate - co2_growth_rate
        > 7: absolute decoupling (GDP growing fast, CO2 falling)
        0-7: relative decoupling
        < 0: no decoupling

Score: 0 = absolute decoupling with strong positive index, 100 = no decoupling.

Sources: World Bank WDI NY.GDP.MKTP.KD.ZG (GDP growth, annual %),
EN.ATM.CO2E.KT (CO2 emissions, kt).
IEA Decoupling Report 2023. OECD Green Growth Indicators.
"""

from __future__ import annotations

import math

import numpy as np

from app.layers.base import LayerBase

_GDP_CODE = "NY.GDP.MKTP.KD.ZG"
_GDP_NAME = "GDP growth"
_CO2_CODE = "EN.ATM.CO2E.KT"
_CO2_NAME = "CO2 emissions"


class GreenGrowthDecoupling(LayerBase):
    layer_id = "lGT"
    name = "Green Growth Decoupling"

    # Decoupling index thresholds
    STRONG_DECOUPLE = 7.0   # GDP growing + CO2 falling meaningfully
    NO_DECOUPLE = 0.0       # GDP and CO2 growing at same rate

    async def compute(self, db, **kwargs) -> dict:
        gdp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_GDP_CODE, f"%{_GDP_NAME}%"),
        )
        co2_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_CO2_CODE, f"%{_CO2_NAME}%"),
        )

        if not gdp_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no GDP growth data (NY.GDP.MKTP.KD.ZG)"}
        if not co2_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no CO2 emissions data (EN.ATM.CO2E.KT)"}

        gdp_vals = [float(r["value"]) for r in gdp_rows if r["value"] is not None]
        co2_vals = [float(r["value"]) for r in co2_rows if r["value"] is not None and float(r["value"]) > 0]

        if len(gdp_vals) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient GDP growth data points"}
        if len(co2_vals) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient CO2 data points"}

        # GDP average annual growth rate (values are already percentages)
        avg_gdp_growth = float(np.mean(gdp_vals))

        # CO2 annual growth rate via log-linear regression
        log_co2 = np.array([math.log(v) for v in co2_vals], dtype=float)
        t = np.arange(len(log_co2), dtype=float)
        co2_slope = float(np.polyfit(t[::-1], log_co2, 1)[0])
        co2_growth_rate = co2_slope * 100  # % per year

        # Decoupling index: higher = better decoupling
        decoupling_index = avg_gdp_growth - co2_growth_rate

        # Scoring: strong_decouple -> 0, at parity -> 50, deep negative -> 100
        if decoupling_index >= self.STRONG_DECOUPLE:
            score = 0.0
        elif decoupling_index >= self.NO_DECOUPLE:
            score = (self.STRONG_DECOUPLE - decoupling_index) / self.STRONG_DECOUPLE * 50
        else:
            # negative decoupling: CO2 growing faster than GDP
            score = min(50 + (-decoupling_index / self.STRONG_DECOUPLE) * 50, 100.0)

        # Classify decoupling state
        if decoupling_index >= self.STRONG_DECOUPLE and co2_growth_rate < 0:
            state = "absolute_decoupling"
        elif decoupling_index >= self.STRONG_DECOUPLE:
            state = "strong_relative_decoupling"
        elif decoupling_index >= 0:
            state = "relative_decoupling"
        else:
            state = "no_decoupling"

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "avg_gdp_growth_pct_yr": round(avg_gdp_growth, 3),
                "co2_growth_rate_pct_yr": round(co2_growth_rate, 3),
                "decoupling_index": round(decoupling_index, 3),
                "decoupling_state": state,
                "gdp_observations": len(gdp_vals),
                "co2_observations": len(co2_vals),
            },
        }
