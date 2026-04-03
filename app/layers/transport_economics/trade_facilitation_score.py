"""Trade Facilitation Score module.

Measures border clearance efficiency using time to import and time to export
(days). Longer clearance times indicate higher trade friction and logistics costs.

Indicators: IC.IMP.DURS (time to import, border compliance, hours),
            IC.EXP.DURS (time to export, border compliance, hours).
Score = clip(avg_hours / 240 * 100, 0, 100) where 240 hours = 10 days frontier.

Sources: WDI IC.IMP.DURS, IC.EXP.DURS (Doing Business / B-READY)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_FRONTIER_HOURS = 240.0  # 10 days = high friction threshold


class TradeFacilitationScore(LayerBase):
    layer_id = "lTR"
    name = "Trade Facilitation Score"

    async def compute(self, db, **kwargs) -> dict:
        imp_code = "IC.IMP.DURS"
        exp_code = "IC.EXP.DURS"

        imp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (imp_code, f"%{imp_code}%"),
        )
        exp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (exp_code, f"%{exp_code}%"),
        )

        if not imp_rows and not exp_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for IC.IMP.DURS or IC.EXP.DURS"}

        components = []
        metrics: dict = {}

        if imp_rows:
            imp_hrs = float(imp_rows[0]["value"])
            components.append(imp_hrs)
            metrics["import_clearance_hours"] = round(imp_hrs, 1)

        if exp_rows:
            exp_hrs = float(exp_rows[0]["value"])
            components.append(exp_hrs)
            metrics["export_clearance_hours"] = round(exp_hrs, 1)

        avg_hours = float(np.mean(components))
        score = float(np.clip(avg_hours / _FRONTIER_HOURS * 100.0, 0, 100))
        metrics["avg_clearance_hours"] = round(avg_hours, 1)
        metrics["frontier_hours"] = _FRONTIER_HOURS

        return {
            "score": round(score, 1),
            "signal": self.classify_signal(score),
            "metrics": metrics,
            "_sources": ["WDI:IC.IMP.DURS", "WDI:IC.EXP.DURS"],
        }
