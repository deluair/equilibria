"""Water Sustainability: composite of water stress, sanitation access, and water access.

Combines three World Bank water-related indicators into a weighted composite score.
Each indicator is gap-scored relative to ideal frontier values. High water stress,
poor sanitation, and low water access all contribute to the overall stress score.

Methodology:
    Dimensions (all higher = worse after gap transformation):
        water_stress_gap  = clip(ER.H2O.FWTL.ZS / 100, 0, 1) * 100
            (freshwater withdrawal as % of total available; higher = more stressed)
        sanitation_gap    = 100 - SH.STA.BASS.ZS
            (% with basic sanitation; gap from 100%)
        water_access_gap  = 100 - SH.H2O.BASW.ZS
            (% with basic water; gap from 100%)

    Weights: water_stress 0.4, sanitation 0.3, water_access 0.3
    score = weighted average of available gaps

References:
    Mekonnen, M. & Hoekstra, A. (2016). "Four billion people facing severe water
        scarcity." Science Advances, 2(2), e1500323.
    WHO/UNICEF (2021). Progress on WASH 2021 Update. WHO, Geneva.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_WEIGHTS = {
    "ER.H2O.FWTL.ZS": 0.4,
    "SH.STA.BASS.ZS": 0.3,
    "SH.H2O.BASW.ZS": 0.3,
}


class WaterSustainability(LayerBase):
    layer_id = "lSU"
    name = "Water Sustainability"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN (
                  'ER.H2O.FWTL.ZS', 'SH.STA.BASS.ZS', 'SH.H2O.BASW.ZS'
              )
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient water sustainability data"}

        series: dict[str, dict[str, float]] = {}
        for r in rows:
            sid = r["series_id"]
            yr = r["date"][:4] if isinstance(r["date"], str) else str(r["date"])
            series.setdefault(sid, {})[yr] = float(r["value"])

        latest: dict[str, float] = {}
        for sid, vals in series.items():
            if vals:
                latest[sid] = vals[max(vals.keys())]

        if not latest:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "no latest values found"}

        gaps: dict[str, float] = {}
        if "ER.H2O.FWTL.ZS" in latest:
            gaps["ER.H2O.FWTL.ZS"] = float(np.clip(latest["ER.H2O.FWTL.ZS"], 0, 100))
        if "SH.STA.BASS.ZS" in latest:
            gaps["SH.STA.BASS.ZS"] = float(np.clip(100 - latest["SH.STA.BASS.ZS"], 0, 100))
        if "SH.H2O.BASW.ZS" in latest:
            gaps["SH.H2O.BASW.ZS"] = float(np.clip(100 - latest["SH.H2O.BASW.ZS"], 0, 100))

        total_weight = sum(_WEIGHTS[sid] for sid in gaps)
        if total_weight == 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid indicators"}

        score = sum(gaps[sid] * _WEIGHTS[sid] for sid in gaps) / total_weight

        return {
            "score": round(float(score), 2),
            "country": country,
            "n_indicators": len(gaps),
            "dimension_gaps": {k: round(v, 2) for k, v in gaps.items()},
            "latest_values": {k: round(v, 3) for k, v in latest.items()},
            "weights": {k: v for k, v in _WEIGHTS.items() if k in gaps},
        }
