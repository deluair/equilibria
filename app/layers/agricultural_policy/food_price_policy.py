"""Food Price Policy module.

Measures food price stabilization effectiveness by comparing domestic food CPI
volatility against headline CPI inflation. High food CPI relative to overall
CPI indicates failed price stabilization policy.

Methodology:
- Query FP.CPI.TOTL.ZG (CPI inflation, annual %) as headline benchmark.
- Compute variance in CPI as proxy for food price instability (food CPI series
  not always available in WDI; FP.CPI.TOTL.ZG is used as the best proxy).
- High CPI volatility -> poor food price stabilization -> higher stress score.
- Score = clip(volatility * 8, 0, 100).

Sources: World Bank WDI (FP.CPI.TOTL.ZG)
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase


class FoodPricePolicy(LayerBase):
    layer_id = "lAP"
    name = "Food Price Policy"

    async def compute(self, db, **kwargs) -> dict:
        code = "FP.CPI.TOTL.ZG"
        name = "CPI inflation"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 10",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no CPI inflation data"}

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if len(values) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data points"}

        avg_inflation = statistics.mean(values)
        inflation_vol = statistics.stdev(values) if len(values) > 1 else 0.0

        # High average inflation and high volatility both signal poor stabilization
        avg_component = min(abs(avg_inflation) * 3.0, 60.0)
        vol_component = min(inflation_vol * 4.0, 40.0)
        score = float(min(avg_component + vol_component, 100.0))

        return {
            "score": round(score, 1),
            "signal": self.classify_signal(score),
            "avg_cpi_inflation_pct": round(avg_inflation, 2),
            "cpi_volatility_std": round(inflation_vol, 3),
            "n_obs": len(values),
            "stabilization_effective": avg_inflation < 5.0 and inflation_vol < 3.0,
            "indicators": [code],
        }
