"""Environmental Governance: regulatory quality and CO2 emissions trend.

Evaluates the effectiveness of environmental governance by combining the World
Bank's regulatory quality estimate with the trend in per capita CO2 emissions.
Weak regulation combined with rising emissions indicates a governance failure on
the environmental dimension.

Methodology:
    rq_latest = latest RQ.EST (regulatory quality, WGI; range approx -2.5 to +2.5)
    Normalize: rq_gap = clip((1.5 - rq_latest) / 3.0 * 50, 0, 50)
        (rq = +1.5 -> rq_gap = 0; rq = -1.5 -> rq_gap = 50)

    co2_slope = OLS slope of EN.ATM.CO2E.PC over time
    emissions_score = clip(co2_slope / 0.5 * 50, 0, 50)
        (0.5 tCO2/pc/yr as upper bound; declining emissions -> 0)

    score = rq_gap + emissions_score

References:
    Kaufmann, D., Kraay, A. & Mastruzzi, M. (2010). "The Worldwide Governance
        Indicators: Methodology and Analytical Issues." World Bank Policy Research
        Working Paper 5430.
    Bimonte, S. (2002). "Information access, income distribution, and the EKC."
        Ecological Economics, 41(1), 145-156.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase

_RQ_MAX = 1.5    # WGI regulatory quality upper anchor
_RQ_MIN = -1.5   # WGI regulatory quality lower anchor
_SLOPE_MAX = 0.5 # tCO2/pc/yr considered severely worsening


class EnvironmentalGovernance(LayerBase):
    layer_id = "lSU"
    name = "Environmental Governance"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('RQ.EST', 'EN.ATM.CO2E.PC')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 4:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient regulatory quality/CO2 data"}

        series: dict[str, dict[str, float]] = {}
        for r in rows:
            sid = r["series_id"]
            yr = r["date"][:4] if isinstance(r["date"], str) else str(r["date"])
            series.setdefault(sid, {})[yr] = float(r["value"])

        rq = series.get("RQ.EST", {})
        co2_pc = series.get("EN.ATM.CO2E.PC", {})

        rq_latest = float(rq[max(rq.keys())]) if rq else 0.0
        rq_gap = float(np.clip((_RQ_MAX - rq_latest) / (_RQ_MAX - _RQ_MIN) * 50, 0, 50))

        emissions_score = 25.0  # default if no trend available
        co2_slope = None
        if len(co2_pc) >= 4:
            sorted_yrs = sorted(co2_pc.keys())
            years = np.array([int(y) for y in sorted_yrs])
            vals = np.array([co2_pc[y] for y in sorted_yrs])
            co2_slope, _, _, _, _ = stats.linregress(years - years[0], vals)
            emissions_score = float(np.clip(co2_slope / _SLOPE_MAX * 50, 0, 50))

        score = float(np.clip(rq_gap + emissions_score, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "regulatory_quality_latest": round(rq_latest, 4),
            "rq_gap": round(rq_gap, 2),
            "co2_pc_trend_slope": round(float(co2_slope), 4) if co2_slope is not None else None,
            "emissions_score": round(emissions_score, 2),
            "n_rq_years": len(rq),
            "n_co2_years": len(co2_pc),
        }
