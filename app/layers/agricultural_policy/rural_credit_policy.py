"""Rural Credit Policy module.

Measures agricultural credit as a share of total domestic credit.
Insufficient credit access in rural areas constrains investment in inputs,
technology, and land improvements.

Methodology:
- Query FS.AST.PRVT.GD.ZS (domestic credit to private sector % GDP).
- Query SL.AGR.EMPL.ZS (agricultural employment % total) as rural demand proxy.
- Rural credit gap proxy: ag employment share / credit penetration ratio.
  High ag employment with low credit penetration -> underserved rural sector.
- Score = clip(gap_ratio * 20, 0, 100).

Sources: World Bank WDI (FS.AST.PRVT.GD.ZS, SL.AGR.EMPL.ZS)
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase


class RuralCreditPolicy(LayerBase):
    layer_id = "lAP"
    name = "Rural Credit Policy"

    async def compute(self, db, **kwargs) -> dict:
        credit_code = "FS.AST.PRVT.GD.ZS"
        credit_name = "domestic credit to private sector"
        credit_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 10",
            (credit_code, f"%{credit_name}%"),
        )

        empl_code = "SL.AGR.EMPL.ZS"
        empl_name = "employment in agriculture"
        empl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 10",
            (empl_code, f"%{empl_name}%"),
        )

        if not credit_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no domestic credit data"}

        credit_vals = [float(r["value"]) for r in credit_rows if r["value"] is not None]
        if not credit_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid credit data"}

        avg_credit = statistics.mean(credit_vals)

        empl_vals = [float(r["value"]) for r in empl_rows if r["value"] is not None]
        avg_empl = statistics.mean(empl_vals) if empl_vals else None

        # Credit gap: ag workforce proportion relative to credit penetration
        # Low credit + high ag employment = high rural credit gap
        if avg_empl is not None and avg_credit > 1e-6:
            gap_ratio = avg_empl / (avg_credit / 10.0 + 1e-6)
        elif avg_credit < 20.0:
            gap_ratio = 3.0  # Low overall credit -> poor rural access
        else:
            gap_ratio = 1.0

        score = float(min(gap_ratio * 20.0, 100.0))

        return {
            "score": round(score, 1),
            "signal": self.classify_signal(score),
            "avg_domestic_credit_pct_gdp": round(avg_credit, 2),
            "avg_ag_employment_pct": round(avg_empl, 2) if avg_empl is not None else None,
            "rural_credit_gap_ratio": round(gap_ratio, 4),
            "n_obs_credit": len(credit_vals),
            "n_obs_empl": len(empl_vals) if empl_vals else 0,
            "indicators": [credit_code, empl_code],
        }
