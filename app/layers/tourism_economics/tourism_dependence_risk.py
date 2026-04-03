"""Tourism Dependence Risk module.

Measures over-reliance on tourism via the coefficient of variation (CoV)
of international tourism receipts (ST.INT.RCPT.XP.ZS). High CoV signals
volatile and concentrated dependence, raising structural vulnerability.

Score: 0 (no risk) to 100 (extreme dependence/volatility).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TourismDependenceRisk(LayerBase):
    layer_id = "lTO"
    name = "Tourism Dependence Risk"

    async def compute(self, db, **kwargs) -> dict:
        code = "ST.INT.RCPT.XP.ZS"
        name = "international tourism receipts"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for tourism dependence risk (ST.INT.RCPT.XP.ZS)",
            }

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if len(values) < 3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient observations for CoV calculation (need >= 3)",
            }

        arr = np.array(values)
        mean_share = float(np.mean(arr))
        std_share = float(np.std(arr, ddof=1))
        cov = std_share / mean_share if mean_share > 0 else 0.0
        latest = values[0]

        # Risk rises with high share AND high volatility
        # share_risk: 0% -> 0, 30% -> 60, 50% -> 100
        share_risk = min(latest * 2.0, 100.0)
        # volatility_risk: CoV 0 -> 0, CoV 1 -> 40
        volatility_risk = min(cov * 40.0, 40.0)
        score = float(np.clip(share_risk * 0.6 + volatility_risk, 0, 100))

        return {
            "score": round(score, 1),
            "indicator": code,
            "latest_pct": round(latest, 2),
            "mean_pct": round(mean_share, 2),
            "std_pct": round(std_share, 2),
            "cov": round(cov, 3),
            "n_obs": len(values),
            "methodology": "score = clip(share * 1.2 + cov * 40, 0, 100)",
        }
