"""Agricultural credit share: credit supply relative to agriculture's economic weight.

Methodology
-----------
**Agricultural credit adequacy** estimated from:
    - NV.AGR.TOTL.ZS: Agriculture, value added (% of GDP) -- economic weight of sector.
    - FS.AST.PRVT.GD.ZS: Domestic credit to private sector (% of GDP) -- total credit pool.

A standard benchmark (FAO / World Bank) is that agriculture should receive credit
roughly proportional to its GDP share. When credit/GDP is low relative to agriculture's
GDP weight, the sector is underfinanced.

    ag_credit_ratio = credit_pct_gdp / ag_value_added_pct_gdp

    If ratio < 1: credit pool is thin compared to sector size -> higher stress.
    If ratio > 3: ample credit relative to sector size -> low stress.

Score (0-100): higher = more stress (worse).
    ratio < 0.5 -> ~90
    ratio ~1.0  -> ~60
    ratio > 3.0 -> ~10

Sources: World Bank WDI (NV.AGR.TOTL.ZS, FS.AST.PRVT.GD.ZS)
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


class AgriculturalCreditShare(LayerBase):
    layer_id = "lAF"
    name = "Agricultural Credit Share"

    async def compute(self, db, **kwargs) -> dict:
        code_ag, name_ag = "NV.AGR.TOTL.ZS", "%agriculture, value added%"
        code_cr, name_cr = "FS.AST.PRVT.GD.ZS", "%domestic credit to private sector%"

        rows_ag = await db.fetch_all(_SQL, (code_ag, name_ag))
        rows_cr = await db.fetch_all(_SQL, (code_cr, name_cr))

        if not rows_ag and not rows_cr:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no agricultural value added or credit data"}

        ag_vals = [float(r["value"]) for r in rows_ag if r["value"] is not None]
        cr_vals = [float(r["value"]) for r in rows_cr if r["value"] is not None]

        ag_share = statistics.mean(ag_vals[:3]) if ag_vals else None
        credit = statistics.mean(cr_vals[:3]) if cr_vals else None

        metrics: dict = {
            "ag_value_added_pct_gdp": round(ag_share, 2) if ag_share is not None else None,
            "credit_to_private_sector_pct_gdp": round(credit, 2) if credit is not None else None,
        }

        if ag_share is None or ag_share < 0.1 or credit is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data", "metrics": metrics}

        ratio = credit / ag_share
        # Higher ratio = more credit per unit of agricultural output = lower stress
        score = max(0.0, min(100.0, 100.0 - 25.0 * ratio))

        metrics["credit_to_ag_value_added_ratio"] = round(ratio, 3)

        return {
            "score": round(score, 2),
            "metrics": metrics,
        }
