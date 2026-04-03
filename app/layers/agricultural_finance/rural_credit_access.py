"""Rural credit access: financial services penetration in agricultural economy.

Methodology
-----------
**Rural credit access** proxied from two WDI indicators:
    - FS.AST.PRVT.GD.ZS: Domestic credit to private sector (% of GDP) -- proxy
      for overall credit availability in the economy.
    - SL.AGR.EMPL.ZS: Employment in agriculture (% of total employment) -- proxy
      for the rural/agricultural share of the workforce that relies on credit access.

When agricultural employment is high relative to private credit supply, the
implied credit per agricultural worker is thin -- signalling poor rural credit
access. The ratio (credit_depth / ag_employment_share) is normalised against
cross-country benchmarks.

Score (0-100): higher = worse rural credit access (more stress).
    credit_ratio < 0.5 -> ~80 (CRISIS)
    credit_ratio ~1.0  -> ~50 (WATCH)
    credit_ratio > 2.0 -> ~10 (STABLE)

Sources: World Bank WDI (FS.AST.PRVT.GD.ZS, SL.AGR.EMPL.ZS)
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


class RuralCreditAccess(LayerBase):
    layer_id = "lAF"
    name = "Rural Credit Access"

    async def compute(self, db, **kwargs) -> dict:
        code_credit, name_credit = "FS.AST.PRVT.GD.ZS", "%domestic credit to private sector%"
        code_ag, name_ag = "SL.AGR.EMPL.ZS", "%employment in agriculture%"

        rows_credit = await db.fetch_all(_SQL, (code_credit, name_credit))
        rows_ag = await db.fetch_all(_SQL, (code_ag, name_ag))

        if not rows_credit and not rows_ag:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no rural credit or agriculture employment data"}

        credit_vals = [float(r["value"]) for r in rows_credit if r["value"] is not None]
        ag_vals = [float(r["value"]) for r in rows_ag if r["value"] is not None]

        credit = statistics.mean(credit_vals[:3]) if credit_vals else None
        ag_share = statistics.mean(ag_vals[:3]) if ag_vals else None

        metrics: dict = {
            "credit_to_private_sector_pct_gdp": round(credit, 2) if credit is not None else None,
            "ag_employment_share_pct": round(ag_share, 2) if ag_share is not None else None,
        }

        if credit is None or ag_share is None or ag_share < 0.1:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data for ratio", "metrics": metrics}

        # credit_ratio: credit depth per unit of agricultural labour pressure
        credit_ratio = credit / ag_share

        # Normalise: ratio 0.5 -> score 80, ratio 1.0 -> 50, ratio 2.5 -> 10
        score = max(0.0, min(100.0, 100.0 - 36.0 * credit_ratio))

        metrics["credit_per_ag_share_ratio"] = round(credit_ratio, 3)

        return {
            "score": round(score, 2),
            "metrics": metrics,
        }
