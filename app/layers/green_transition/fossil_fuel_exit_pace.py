"""Fossil fuel exit pace: EG.USE.COMM.FO.ZS — fossil fuel share decline rate.

Methodology
-----------
Fossil fuels as a share of total energy use (EG.USE.COMM.FO.ZS) captures how
quickly an economy is exiting fossil fuel dependency across all energy sectors
(electricity, heat, transport), not just power generation.

IEA NZE 2050 implies fossil fuel share must fall from ~80% (2020) to ~20% by
2050, requiring roughly 2 percentage points per year reduction.

    exit_pace = -(slope of EG.USE.COMM.FO.ZS over time)  [pp/yr, positive = declining]
    nze_pace  = 2.0 pp/yr

Score: 0 = exiting at 2+ pp/yr (NZE-compatible), 100 = no exit or share rising.

Sources: World Bank WDI EG.USE.COMM.FO.ZS (fossil fuel energy consumption, % total).
IEA Net Zero by 2050 (2021).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_CODE = "EG.USE.COMM.FO.ZS"
_NAME = "Fossil fuel energy consumption"


class FossilFuelExitPace(LayerBase):
    layer_id = "lGT"
    name = "Fossil Fuel Exit Pace"

    NZE_PACE_PP_YR = 2.0  # pp/yr decline required (IEA NZE)

    async def compute(self, db, **kwargs) -> dict:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_CODE, f"%{_NAME}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no fossil fuel share data (EG.USE.COMM.FO.ZS)"}

        vals = [float(r["value"]) for r in rows if r["value"] is not None]
        if len(vals) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient fossil fuel share data points"}

        arr = np.array(vals, dtype=float)
        t = np.arange(len(arr), dtype=float)
        slope = float(np.polyfit(t[::-1], arr, 1)[0])  # pp/yr (negative = declining)

        exit_pace = -slope  # positive = declining fossil share (good)
        latest_share = vals[0]
        nze_pace = self.NZE_PACE_PP_YR

        if exit_pace >= nze_pace:
            score = 0.0
        elif exit_pace >= 0:
            score = (nze_pace - exit_pace) / nze_pace * 60
        else:
            # rising fossil share: 60 + penalty
            score = min(60 + (-exit_pace / nze_pace) * 40, 100)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "fossil_fuel_share_pct": round(latest_share, 2),
                "exit_pace_pp_yr": round(exit_pace, 3),
                "nze_required_pace_pp_yr": nze_pace,
                "pace_gap_pp_yr": round(nze_pace - exit_pace, 3),
                "observations": len(vals),
            },
        }
