"""Renewable energy speed: EG.ELC.RNEW.ZS growth rate vs Paris-compatible pace.

Methodology
-----------
IEA Net Zero by 2050 requires renewable electricity to rise from ~29% (2020)
to ~88% by 2030 globally, implying an annual increase of roughly 5-6 percentage
points per year in the electricity mix. The growth rate here is estimated as the
average annual change in renewable electricity share (EG.ELC.RNEW.ZS).

    annual_gain = slope of EG.ELC.RNEW.ZS over available years (pp/yr)
    paris_pace  = 5.9 pp/yr (IEA NZE 2021, global aggregate)

Score: 0 = meeting or exceeding Paris pace, 100 = no renewable penetration gain
or declining share.

Sources: World Bank WDI EG.ELC.RNEW.ZS (renewable electricity output, % total).
IEA Net Zero by 2050 (2021). IRENA World Energy Transitions Outlook 2023.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_CODE = "EG.ELC.RNEW.ZS"
_NAME = "Renewable electricity output"


class RenewableEnergySpeed(LayerBase):
    layer_id = "lGT"
    name = "Renewable Energy Speed"

    PARIS_PACE_PP_YR = 5.9  # percentage points per year (IEA NZE)

    async def compute(self, db, **kwargs) -> dict:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_CODE, f"%{_NAME}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no renewable electricity share data (EG.ELC.RNEW.ZS)"}

        vals = [float(r["value"]) for r in rows if r["value"] is not None]
        if len(vals) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient renewable electricity data points"}

        arr = np.array(vals, dtype=float)
        t = np.arange(len(arr), dtype=float)
        # rows DESC, t=0 most recent -> reverse for chronological slope
        slope = float(np.polyfit(t[::-1], arr, 1)[0])  # pp per year

        latest_share = vals[0]
        paris_pace = self.PARIS_PACE_PP_YR

        # Score: annual gain >= paris_pace -> 0; no gain or falling -> 100
        if slope >= paris_pace:
            score = 0.0
        elif slope >= 0:
            score = (paris_pace - slope) / paris_pace * 70
        else:
            # declining share: 70 + penalty up to 30
            score = min(70 + (-slope / paris_pace) * 30, 100)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "renewable_electricity_share_pct": round(latest_share, 2),
                "annual_gain_pp_yr": round(slope, 3),
                "paris_compatible_pace_pp_yr": paris_pace,
                "pace_gap_pp_yr": round(paris_pace - slope, 3),
                "observations": len(vals),
            },
        }
