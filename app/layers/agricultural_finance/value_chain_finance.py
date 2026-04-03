"""Value chain finance: credit enabling agri-export value chains.

Methodology
-----------
**Value chain finance adequacy** proxied from:
    - NE.EXP.GNFS.ZS: Exports of goods and services (% of GDP) -- export orientation
      proxy; high export share implies significant agri-export value chains needing finance.
    - FS.AST.PRVT.GD.ZS: Domestic credit to private sector (% of GDP) -- financial depth
      to support value chain actors (traders, processors, exporters).

Value chain finance is most critical when exports are large but credit is thin.
The stress indicator captures this mismatch:

    vc_stress = export_share / credit_share

    Low ratio (credit ample for export activity) -> low stress.
    High ratio (exports outpace credit supply) -> high stress.

Score (0-100): higher = worse value chain finance conditions.
    vc_stress < 0.5 -> ~10
    vc_stress ~1.0  -> ~40
    vc_stress > 2.0 -> ~80+

Sources: World Bank WDI (NE.EXP.GNFS.ZS, FS.AST.PRVT.GD.ZS)
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


class ValueChainFinance(LayerBase):
    layer_id = "lAF"
    name = "Value Chain Finance"

    async def compute(self, db, **kwargs) -> dict:
        code_exp, name_exp = "NE.EXP.GNFS.ZS", "%exports of goods and services%"
        code_cr, name_cr = "FS.AST.PRVT.GD.ZS", "%domestic credit to private sector%"

        rows_exp = await db.fetch_all(_SQL, (code_exp, name_exp))
        rows_cr = await db.fetch_all(_SQL, (code_cr, name_cr))

        if not rows_exp and not rows_cr:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no export or credit data"}

        exp_vals = [float(r["value"]) for r in rows_exp if r["value"] is not None]
        cr_vals = [float(r["value"]) for r in rows_cr if r["value"] is not None]

        export_share = statistics.mean(exp_vals[:3]) if exp_vals else None
        credit = statistics.mean(cr_vals[:3]) if cr_vals else None

        metrics: dict = {
            "exports_pct_gdp": round(export_share, 2) if export_share is not None else None,
            "credit_pct_gdp": round(credit, 2) if credit is not None else None,
        }

        if export_share is None or credit is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data for ratio", "metrics": metrics}

        if credit < 0.1:
            return {"score": 95.0, "signal": "CRISIS", "metrics": metrics,
                    "error": "near-zero credit depth"}

        vc_stress = export_share / credit
        score = max(0.0, min(100.0, vc_stress * 40.0))

        metrics["vc_stress_ratio"] = round(vc_stress, 4)

        return {
            "score": round(score, 2),
            "metrics": metrics,
        }
