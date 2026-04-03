"""Decarbonization rate: speed of CO2 reduction (EN.ATM.CO2E.KT trend).

Methodology
-----------
The decarbonization rate measures how quickly a country is reducing absolute
CO2 emissions. A Paris-compatible 1.5C pathway requires global emissions to
fall roughly 7-8% per year from 2020 onwards (IPCC AR6). The annual rate of
change is estimated via log-linear regression on the CO2 emissions time series.

    decarbonization_rate = -(slope of log(CO2) over time) * 100
    Positive rate = emissions falling (good). Negative = rising (bad).

Score: 0 = decarbonizing at 7%+/yr (Paris-compatible), 100 = emissions
growing 5%+/yr (accelerating carbon lock-in).

Sources: World Bank WDI EN.ATM.CO2E.KT (CO2 emissions, kt).
IPCC AR6 WG3 SPM, 2022. IEA Net Zero by 2050, 2021.
"""

from __future__ import annotations

import math

import numpy as np

from app.layers.base import LayerBase

_CODE = "EN.ATM.CO2E.KT"
_NAME = "CO2 emissions"


class DecarbonizationRate(LayerBase):
    layer_id = "lGT"
    name = "Decarbonization Rate"

    async def compute(self, db, **kwargs) -> dict:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_CODE, f"%{_NAME}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no CO2 emissions data (EN.ATM.CO2E.KT)"}

        vals = [float(r["value"]) for r in rows if r["value"] is not None and float(r["value"]) > 0]
        if len(vals) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient CO2 data points"}

        log_vals = np.array([math.log(v) for v in vals], dtype=float)
        t = np.arange(len(log_vals), dtype=float)
        # rows are DESC so t=0 is most recent; reverse for chronological slope
        slope = float(np.polyfit(t[::-1], log_vals, 1)[0])

        # Annual rate of change (positive = growing emissions, negative = falling)
        annual_pct_change = slope * 100

        # Decarbonization rate: negative of change (positive = improving)
        decarb_rate = -annual_pct_change

        # Score: 7%+ annual fall = 0, flat = 50, 5%+ rise = 100
        paris_pace = 7.0
        if decarb_rate >= paris_pace:
            score = 0.0
        elif decarb_rate >= 0:
            score = (paris_pace - decarb_rate) / paris_pace * 50
        else:
            # rising emissions: 0 to +5% rise maps to 50-100
            score = 50 + min(-decarb_rate / 5.0 * 50, 50)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "co2_annual_pct_change": round(annual_pct_change, 3),
                "decarbonization_rate_pct_yr": round(decarb_rate, 3),
                "paris_compatible_pace_pct_yr": paris_pace,
                "observations": len(vals),
                "latest_co2_kt": round(vals[0], 0),
            },
        }
