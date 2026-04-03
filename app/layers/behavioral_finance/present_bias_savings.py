"""Present Bias Savings module.

Household savings rate vs optimal benchmark. Low gross national savings
relative to income indicates present-biased preferences: households
under-save relative to what lifecycle models predict.

Sources: WDI NY.GNS.ICTR.ZS (gross savings % of GNI)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# Rough optimal savings benchmark from Solow steady-state / lifecycle literature
OPTIMAL_SAVINGS_RATE = 22.0  # percent of GNI


class PresentBiasSavings(LayerBase):
    layer_id = "lBF"
    name = "Present Bias Savings"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("NY.GNS.ICTR.ZS", "%gross savings%"),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        vals = np.array([float(r["value"]) for r in rows])
        mean_savings = float(np.mean(vals))
        recent_savings = float(vals[0])  # most recent (DESC order)
        deficit = OPTIMAL_SAVINGS_RATE - mean_savings

        # Larger savings deficit = stronger present bias
        score = float(np.clip(max(0.0, deficit) * 3.5, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(rows),
            "mean_savings_rate_pct": round(mean_savings, 2),
            "recent_savings_rate_pct": round(recent_savings, 2),
            "optimal_benchmark_pct": OPTIMAL_SAVINGS_RATE,
            "savings_deficit": round(deficit, 2),
            "interpretation": "Savings below optimal benchmark signals present-biased under-saving",
        }
