"""Policy ambition gap: EN.ATM.CO2E.PC trend vs Paris-compatible per-capita targets.

Methodology
-----------
The policy ambition gap measures the divergence between a country's actual
per-capita emissions trajectory and what is needed under the Paris Agreement.

The IPCC AR6 (2022) indicates global per-capita CO2 emissions must fall from
~4.7 tCO2/person (2019) to approximately 2.0 tCO2/person by 2030 and ~0.7 by
2050 for a 1.5C pathway. The annual reduction required from 2020 is roughly
0.27 tCO2/person/year.

This module:
1. Fetches per-capita CO2 (EN.ATM.CO2E.PC, metric tons per capita)
2. Estimates the actual annual change via linear regression
3. Computes the gap between actual trajectory and Paris-required reduction

    paris_required_reduction = 0.27 tCO2/person/yr
    actual_change = slope of EN.ATM.CO2E.PC over time
    ambition_gap = actual_change - (-paris_required_reduction)
                 = actual_change + 0.27  [positive = behind Paris; negative = ahead]

Score: 0 = meeting or exceeding Paris reduction pace, 100 = emissions growing or
far behind required reductions.

Sources: World Bank WDI EN.ATM.CO2E.PC (CO2 emissions, metric tons per capita).
IPCC AR6 Working Group III Summary for Policymakers (2022).
Climate Action Tracker methodology.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_CODE = "EN.ATM.CO2E.PC"
_NAME = "CO2 emissions (metric tons per capita)"

# IPCC AR6 WG3 SPM: ~7-8% global emissions reduction/yr from 2020 for 1.5C
# Translates to ~0.27 tCO2/person/yr absolute reduction from 2019 baseline (~4.7t)
_PARIS_REDUCTION_PER_YR = 0.27  # tCO2/person/yr


class PolicyAmbitionGap(LayerBase):
    layer_id = "lGT"
    name = "Policy Ambition Gap"

    PARIS_REQUIRED_REDUCTION = _PARIS_REDUCTION_PER_YR

    async def compute(self, db, **kwargs) -> dict:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_CODE, f"%{_NAME}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no per-capita CO2 data (EN.ATM.CO2E.PC)"}

        vals = [float(r["value"]) for r in rows if r["value"] is not None]
        if len(vals) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient per-capita CO2 data points"}

        arr = np.array(vals, dtype=float)
        t = np.arange(len(arr), dtype=float)
        # rows DESC -> reverse for chronological slope
        slope = float(np.polyfit(t[::-1], arr, 1)[0])  # tCO2/person/yr (negative = falling)

        # Ambition gap: positive means actual trend is above (worse than) Paris pace
        ambition_gap = slope + self.PARIS_REQUIRED_REDUCTION

        # Score: gap <= 0 (on track or better) = 0; gap = 0.5+ = 100
        if ambition_gap <= 0:
            score = 0.0
        elif ambition_gap >= 0.5:
            score = 100.0
        else:
            score = (ambition_gap / 0.5) * 100

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "co2_per_capita_latest_t": round(vals[0], 3),
                "actual_annual_change_t_yr": round(slope, 4),
                "paris_required_reduction_t_yr": self.PARIS_REQUIRED_REDUCTION,
                "ambition_gap_t_yr": round(ambition_gap, 4),
                "observations": len(vals),
            },
        }
