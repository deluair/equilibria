"""Loss Aversion Premium module.

Excess risk aversion in financial markets proxied by the real interest rate.
Persistently high real interest rates suggest lenders demand an outsized
premium to compensate for perceived downside risk, consistent with loss
aversion theory.

Sources: WDI FR.INR.RINR (real interest rate %)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class LossAversionPremium(LayerBase):
    layer_id = "lBF"
    name = "Loss Aversion Premium"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("FR.INR.RINR", "%real interest rate%"),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        vals = np.array([float(r["value"]) for r in rows])
        mean_real_rate = float(np.mean(vals))
        std_real_rate = float(np.std(vals))
        pct_positive_high = float(np.mean(vals > 5.0) * 100)

        # High and volatile real rates = strong loss aversion premium
        level_score = np.clip(max(0.0, mean_real_rate) * 4, 0, 60)
        vol_score = np.clip(std_real_rate * 3, 0, 40)
        score = float(level_score + vol_score)

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(rows),
            "mean_real_interest_rate": round(mean_real_rate, 3),
            "std_real_interest_rate": round(std_real_rate, 3),
            "pct_obs_above_5pct": round(pct_positive_high, 1),
            "interpretation": "Persistently high real rates reflect excess risk aversion / loss aversion premium",
        }
