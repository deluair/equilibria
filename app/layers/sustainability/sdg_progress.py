"""SDG Progress: composite proxy across poverty, education, health, clean energy.

Constructs a Sustainable Development Goal shortfall index using World Bank WDI
indicators as proxies for four core SDG dimensions. Each dimension is normalized
to a 0-100 gap score (distance from the frontier), and the arithmetic mean of
these gaps is the SDG shortfall score.

Methodology:
    Frontier values are set to targets consistent with SDG commitments:
        - Poverty: 0% below $2.15/day (SDG 1)
        - Primary completion: 100% (SDG 4)
        - Life expectancy: 85 years (SDG 3)
        - Electricity access: 100% (SDG 7)

    Dimension gap = (frontier - observed) / (frontier - worst_case) * 100
    Shortfall score = mean(dimension_gaps), clipped to [0, 100].

References:
    Sachs, J. et al. (2023). Sustainable Development Report 2023. SDSN.
    Vollmer, S. & Alkire, S. (2022). "Towards a unified framework for SDG
        progress assessment." World Development, 147, 105606.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# Frontier (best-case) and worst-case anchors for each indicator
_ANCHORS = {
    "SI.POV.DDAY": {"frontier": 0.0, "worst": 80.0},      # poverty headcount %
    "SE.PRM.CMPT.ZS": {"frontier": 100.0, "worst": 20.0}, # primary completion %
    "SH.DYN.LE00.IN": {"frontier": 85.0, "worst": 40.0},  # life expectancy years
    "EG.ELC.ACCS.ZS": {"frontier": 100.0, "worst": 0.0},  # electricity access %
}


class SDGProgress(LayerBase):
    layer_id = "lSU"
    name = "SDG Progress"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN (
                  'SI.POV.DDAY', 'SE.PRM.CMPT.ZS',
                  'SH.DYN.LE00.IN', 'EG.ELC.ACCS.ZS'
              )
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 4:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient SDG proxy data"}

        series: dict[str, dict[str, float]] = {}
        for r in rows:
            sid = r["series_id"]
            yr = r["date"][:4] if isinstance(r["date"], str) else str(r["date"])
            series.setdefault(sid, {})[yr] = float(r["value"])

        # Take the most recent available value for each indicator
        latest: dict[str, float] = {}
        for sid, vals in series.items():
            if vals:
                latest[sid] = vals[max(vals.keys())]

        if len(latest) < 2:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "fewer than 2 SDG indicators available"}

        gaps: list[float] = []
        dimension_scores: dict[str, float] = {}
        for sid, anchors in _ANCHORS.items():
            if sid not in latest:
                continue
            val = latest[sid]
            frontier = anchors["frontier"]
            worst = anchors["worst"]
            # For indicators where higher = better (all except poverty)
            if sid == "SI.POV.DDAY":
                gap = np.clip((val - frontier) / (worst - frontier) * 100, 0, 100)
            else:
                gap = np.clip((frontier - val) / (frontier - worst) * 100, 0, 100)
            gaps.append(float(gap))
            dimension_scores[sid] = round(float(gap), 2)

        score = float(np.mean(gaps))

        return {
            "score": round(score, 2),
            "country": country,
            "n_indicators": len(gaps),
            "dimension_gaps": dimension_scores,
            "latest_values": {k: round(v, 3) for k, v in latest.items()},
            "interpretation": "higher score = greater SDG shortfall",
        }
