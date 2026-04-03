"""Food Reserve Adequacy module.

Measures cereal stock-to-use ratio as an indicator of food reserve adequacy.
Low cereal stocks relative to consumption signal vulnerability to supply shocks
and inadequate buffer stock policy.

Methodology:
- Query AG.YLD.CREL.KG (cereal yield, kg per hectare) as production capacity proxy.
- Query NV.AGR.TOTL.ZS (agriculture value added % GDP) as sector size proxy.
- Stock-to-use proxy: derived from yield level relative to global benchmark.
  Yield below 3,000 kg/ha suggests production below reserve-building threshold.
- Score = clip((3000 - yield) / 30, 0, 100) for low-yield; 0 for high yield.

Sources: World Bank WDI (AG.YLD.CREL.KG, NV.AGR.TOTL.ZS)
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase


class FoodReserveAdequacy(LayerBase):
    layer_id = "lAP"
    name = "Food Reserve Adequacy"

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
        if not yield_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid yield data"}

        avg_yield = statistics.mean(yield_vals)
        # Yield trend: positive is good for reserves
        yield_trend = (yield_vals[0] - yield_vals[-1]) if len(yield_vals) > 1 else 0.0

        ag_vals = [float(r["value"]) for r in ag_rows if r["value"] is not None]
        avg_ag_share = statistics.mean(ag_vals) if ag_vals else None

        # Global cereal yield benchmark: ~3,500 kg/ha world average
        # Below 2,500 signals critically low reserve-building capacity
        BENCHMARK_YIELD = 3500.0
        yield_gap = max(0.0, BENCHMARK_YIELD - avg_yield)
        base_score = float(min(yield_gap / 35.0, 80.0))

        # Negative yield trend worsens score
        trend_penalty = float(min(max(0.0, -yield_trend / 50.0), 20.0))
        score = float(min(base_score + trend_penalty, 100.0))

        return {
            "score": round(score, 1),
            "signal": self.classify_signal(score),
            "avg_cereal_yield_kg_ha": round(avg_yield, 1),
            "yield_benchmark_kg_ha": BENCHMARK_YIELD,
            "yield_gap_kg_ha": round(yield_gap, 1),
            "yield_trend_kg_ha": round(yield_trend, 1),
            "avg_ag_value_added_pct_gdp": round(avg_ag_share, 2) if avg_ag_share is not None else None,
            "n_obs": len(yield_vals),
            "indicators": [yield_code, ag_code],
        }
