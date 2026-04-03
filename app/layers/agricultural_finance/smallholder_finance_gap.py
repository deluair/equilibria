"""Smallholder finance gap: rural poverty-credit demand intersection.

Methodology
-----------
**Smallholder finance gap** estimated from:
    - SL.AGR.EMPL.ZS: Employment in agriculture (% of total employment) -- proxy
      for the population of smallholder farmers demanding credit.
    - SI.POV.DDAY: Poverty headcount ratio at $2.15/day (% of population) --
      indicates the proportion unable to self-finance inputs, most of whom reside
      in rural/agricultural areas.

    The gap is the joint stress of having both high agricultural employment
    (large smallholder population) and high poverty rates (limited self-finance):

    gap_index = (ag_employment_pct + poverty_rate_pct) / 2

    This joint index maps to the effective population excluded from formal
    agricultural credit (IFAD 2021 estimates: 500M+ smallholders lack credit).

Score (0-100): higher = larger smallholder finance gap (more stress).
    gap_index > 70 -> ~90+
    gap_index ~40  -> ~55
    gap_index < 10 -> ~10

Sources: World Bank WDI (SL.AGR.EMPL.ZS, SI.POV.DDAY)
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase

_SQL = """
    SELECT value FROM data_points
    WHERE series_id = (
        SELECT id FROM data_series
        WHERE indicator_code = ? OR name LIKE ?
    )
    ORDER BY date DESC LIMIT 15
"""


class SmallholderFinanceGap(LayerBase):
    layer_id = "lAF"
    name = "Smallholder Finance Gap"

    async def compute(self, db, **kwargs) -> dict:
        code_ag, name_ag = "SL.AGR.EMPL.ZS", "%employment in agriculture%"
        code_pov, name_pov = "SI.POV.DDAY", "%poverty headcount ratio%"

        rows_ag = await db.fetch_all(_SQL, (code_ag, name_ag))
        rows_pov = await db.fetch_all(_SQL, (code_pov, name_pov))

        if not rows_ag and not rows_pov:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no agriculture employment or poverty data"}

        ag_vals = [float(r["value"]) for r in rows_ag if r["value"] is not None]
        pov_vals = [float(r["value"]) for r in rows_pov if r["value"] is not None]

        ag_share = statistics.mean(ag_vals[:3]) if ag_vals else None
        poverty = statistics.mean(pov_vals[:3]) if pov_vals else None

        metrics: dict = {
            "ag_employment_share_pct": round(ag_share, 2) if ag_share is not None else None,
            "poverty_headcount_pct": round(poverty, 2) if poverty is not None else None,
        }

        if ag_share is None and poverty is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable data", "metrics": metrics}

        # If only one indicator, use it as sole driver with reduced weight
        if ag_share is not None and poverty is None:
            gap_index = ag_share * 0.6
        elif poverty is not None and ag_share is None:
            gap_index = poverty * 0.6
        else:
            gap_index = (ag_share + poverty) / 2.0

        score = max(0.0, min(100.0, gap_index * 1.3))

        metrics["smallholder_gap_index"] = round(gap_index, 2)

        return {
            "score": round(score, 2),
            "metrics": metrics,
        }
