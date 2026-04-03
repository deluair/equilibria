"""Transaction Cost Index module.

Measures trade facilitation costs as proxy for economy-wide transaction costs
using World Bank Doing Business indicators:
1. IC.EXP.DURS: Time to export (days) -- border compliance.
2. IC.IMP.DURS: Time to import (days) -- border compliance.

High trade facilitation costs signal poor logistics infrastructure, customs
inefficiency, bureaucratic delays, and overall high transaction costs in the
economy. These raise prices, reduce trade volumes, and deter FDI.

Benchmark: OECD average export ~12h, import ~24h. Values in hours converted to
days where needed. Stress mapped nonlinearly against regional benchmarks.

References:
    World Bank. (2023). Doing Business / Business Ready Indicators.
    Anderson, J.E. & van Wincoop, E. (2004). Trade Costs. JEL 42(3), 691-751.
    Hummels, D. (2007). Transportation Costs and International Trade. JEL 45(3).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class TransactionCostIndex(LayerBase):
    layer_id = "lIE"
    name = "Transaction Cost Index"

    async def compute(self, db, **kwargs) -> dict:
        exp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("IC.EXP.DURS", "%time to export%"),
        )
        imp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("IC.IMP.DURS", "%time to import%"),
        )

        if not exp_rows and not imp_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no trade facilitation data"}

        def days_stress(days: float) -> float:
            # 0-3 days: minimal stress; 3-10 moderate; 10-30 high; >30 severe
            if days <= 3:
                return days / 3.0 * 0.20
            elif days <= 10:
                return 0.20 + (days - 3) / 7.0 * 0.30
            elif days <= 30:
                return 0.50 + (days - 10) / 20.0 * 0.35
            else:
                return min(0.85 + (days - 30) / 30.0 * 0.15, 1.0)

        metrics = {}
        stresses = []

        if exp_rows:
            exp_days = float(exp_rows[0]["value"])
            s = days_stress(exp_days)
            stresses.append(s)
            metrics["export_days"] = round(exp_days, 1)
            metrics["export_stress"] = round(s, 4)

        if imp_rows:
            imp_days = float(imp_rows[0]["value"])
            s = days_stress(imp_days)
            stresses.append(s)
            metrics["import_days"] = round(imp_days, 1)
            metrics["import_stress"] = round(s, 4)

        composite_stress = sum(stresses) / len(stresses)
        score = round(composite_stress * 100.0, 2)
        metrics["n_indicators"] = len(stresses)

        return {
            "score": score,
            "metrics": metrics,
            "reference": "WB IC.EXP.DURS + IC.IMP.DURS; Anderson & van Wincoop 2004 JEL",
        }
