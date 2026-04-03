"""Irrigation Policy Gap module.

Measures the gap between irrigated land share and agricultural water needs.
Underinvestment in irrigation infrastructure signals a policy gap that
constrains agricultural productivity and food security.

Methodology:
- Query AG.LND.IRIG.AG.ZS (irrigated land % of agricultural land).
- Query SL.AGR.EMPL.ZS (agricultural employment % total) as demand proxy.
- Policy gap = agricultural employment share - irrigated land share (when positive).
- High gap -> irrigation policy underinvestment -> higher score.
- Score = clip(gap * 1.5, 0, 100).

Sources: World Bank WDI (AG.LND.IRIG.AG.ZS, SL.AGR.EMPL.ZS)
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase


class IrrigationPolicyGap(LayerBase):
    layer_id = "lAP"
    name = "Irrigation Policy Gap"

    async def compute(self, db, **kwargs) -> dict:
        irrig_code = "AG.LND.IRIG.AG.ZS"
        irrig_name = "irrigated land"
        irrig_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 10",
            (irrig_code, f"%{irrig_name}%"),
        )

        empl_code = "SL.AGR.EMPL.ZS"
        empl_name = "employment in agriculture"
        empl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 10",
            (empl_code, f"%{empl_name}%"),
        )

        if not irrig_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no irrigated land data"}

        irrig_vals = [float(r["value"]) for r in irrig_rows if r["value"] is not None]
        if not irrig_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid irrigated land data"}

        avg_irrig = statistics.mean(irrig_vals)

        empl_vals = [float(r["value"]) for r in empl_rows if r["value"] is not None]
        avg_empl = statistics.mean(empl_vals) if empl_vals else None

        # Irrigation gap: if ag employment high but irrigation low, policy gap exists
        if avg_empl is not None:
            gap = max(0.0, avg_empl - avg_irrig)
        else:
            # Without employment data, score based on absolute irrigated land level
            # Low irrigation % (e.g., <20%) signals potential gap
            gap = max(0.0, 30.0 - avg_irrig)

        score = float(min(gap * 1.5, 100.0))

        return {
            "score": round(score, 1),
            "signal": self.classify_signal(score),
            "avg_irrigated_land_pct": round(avg_irrig, 2),
            "avg_ag_employment_pct": round(avg_empl, 2) if avg_empl is not None else None,
            "irrigation_policy_gap": round(gap, 2),
            "n_obs_irrig": len(irrig_vals),
            "n_obs_empl": len(empl_vals) if empl_vals else 0,
            "indicators": [irrig_code, empl_code],
        }
