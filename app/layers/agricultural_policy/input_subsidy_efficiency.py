"""Input Subsidy Efficiency module.

Measures fertilizer and seed subsidy cost relative to yield improvement.
Uses cereal yield growth as the productivity outcome proxy. Low yield growth
despite high agricultural employment signals inefficient input subsidies.

Methodology:
- Query AG.YLD.CREL.KG (cereal yield, kg per hectare).
- Query SL.AGR.EMPL.ZS (agricultural employment % total employment) as
  proxy for input subsidy absorption capacity.
- Compute yield growth trend over available periods.
- High ag employment share with stagnant yield -> low subsidy efficiency -> higher score.
- Score = clip((1 - yield_growth_norm) * 60 + ag_empl_penalty, 0, 100).

Sources: World Bank WDI (AG.YLD.CREL.KG, SL.AGR.EMPL.ZS)
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase


class InputSubsidyEfficiency(LayerBase):
    layer_id = "lAP"
    name = "Input Subsidy Efficiency"

    async def compute(self, db, **kwargs) -> dict:
        yield_code = "AG.YLD.CREL.KG"
        yield_name = "cereal yield"
        yield_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 10",
            (yield_code, f"%{yield_name}%"),
        )

        empl_code = "SL.AGR.EMPL.ZS"
        empl_name = "employment in agriculture"
        empl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 10",
            (empl_code, f"%{empl_name}%"),
        )

        if not yield_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no cereal yield data"}

        yield_vals = [float(r["value"]) for r in yield_rows if r["value"] is not None]
        if len(yield_vals) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient yield data"}

        # Yield growth: compare most recent to oldest in window
        yield_growth_pct = ((yield_vals[0] - yield_vals[-1]) / (abs(yield_vals[-1]) + 1e-10)) * 100
        avg_yield = statistics.mean(yield_vals)

        empl_vals = [float(r["value"]) for r in empl_rows if r["value"] is not None]
        avg_ag_empl = statistics.mean(empl_vals) if empl_vals else None

        # Negative/stagnant yield growth -> poor subsidy efficiency
        inefficiency = max(0.0, -yield_growth_pct * 2.0)
        stagnation_score = float(min(inefficiency, 60.0))

        # High ag employment share with low yields suggests subsidy waste
        empl_penalty = 0.0
        if avg_ag_empl is not None:
            empl_penalty = min(avg_ag_empl * 0.5, 40.0)

        score = float(min(stagnation_score + empl_penalty, 100.0))

        return {
            "score": round(score, 1),
            "signal": self.classify_signal(score),
            "yield_growth_pct": round(yield_growth_pct, 2),
            "avg_cereal_yield_kg_ha": round(avg_yield, 1),
            "avg_ag_employment_pct": round(avg_ag_empl, 2) if avg_ag_empl is not None else None,
            "n_obs_yield": len(yield_vals),
            "n_obs_empl": len(empl_vals) if empl_vals else 0,
            "indicators": [yield_code, empl_code],
        }
