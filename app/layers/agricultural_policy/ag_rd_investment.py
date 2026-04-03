"""Agricultural R&D Investment module.

Measures agricultural research and development spending as a share of
agricultural GDP. Underinvestment in ag R&D constrains long-run productivity
growth and technology adoption.

Methodology:
- Query AG.YLD.CREL.KG (cereal yield, kg per hectare) as R&D outcome proxy.
- Query NV.AGR.TOTL.ZS (agriculture value added % GDP) as ag sector size.
- R&D investment proxy: yield growth rate relative to sector size.
  Stagnant yield in a large ag sector implies low R&D investment.
- Score = clip((1 - yield_growth_adj) * 70 + underfunding_penalty, 0, 100).

Sources: World Bank WDI (AG.YLD.CREL.KG, NV.AGR.TOTL.ZS)
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase


class AgRdInvestment(LayerBase):
    layer_id = "lAP"
    name = "Agricultural R&D Investment"

    async def compute(self, db, **kwargs) -> dict:
        yield_code = "AG.YLD.CREL.KG"
        yield_name = "cereal yield"
        yield_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 10",
            (yield_code, f"%{yield_name}%"),
        )

        ag_code = "NV.AGR.TOTL.ZS"
        ag_name = "agriculture value added"
        ag_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 10",
            (ag_code, f"%{ag_name}%"),
        )

        if not yield_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no cereal yield data"}

        yield_vals = [float(r["value"]) for r in yield_rows if r["value"] is not None]
        if len(yield_vals) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient yield data for trend"}

        # Annualized yield growth over available window
        n = len(yield_vals)
        yield_growth_pct = ((yield_vals[0] - yield_vals[-1]) / (abs(yield_vals[-1]) + 1e-10)) * 100.0 / n

        ag_vals = [float(r["value"]) for r in ag_rows if r["value"] is not None]
        avg_ag_share = statistics.mean(ag_vals) if ag_vals else None

        # Low/negative yield growth -> low R&D investment outcome
        rd_gap = max(0.0, 2.0 - yield_growth_pct)  # 2% annual yield growth = benchmark
        base_score = float(min(rd_gap * 20.0, 70.0))

        # Large ag sector with slow yield growth -> underfunded R&D
        underfunding_penalty = 0.0
        if avg_ag_share is not None and avg_ag_share > 10.0 and yield_growth_pct < 1.0:
            underfunding_penalty = min((avg_ag_share - 10.0) * 1.5, 30.0)

        score = float(min(base_score + underfunding_penalty, 100.0))

        return {
            "score": round(score, 1),
            "signal": self.classify_signal(score),
            "annualized_yield_growth_pct": round(yield_growth_pct, 3),
            "yield_growth_benchmark_pct": 2.0,
            "rd_gap": round(rd_gap, 3),
            "avg_ag_value_added_pct_gdp": round(avg_ag_share, 2) if avg_ag_share is not None else None,
            "n_obs": n,
            "indicators": [yield_code, ag_code],
        }
