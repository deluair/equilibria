"""Welfare State Maturity module.

Evaluates the depth and stability of a welfare state by analyzing the
time-series trend of social spending. A mature welfare state maintains
consistently high and stable social transfers over many years.

Maturity score rewards high spending levels and penalizes high volatility
(coefficient of variation).

Score = clip(100 - (mean_transfers / 20 * 60) + (cv * 40), 0, 100)
(higher score = less mature)

Sources: WDI GC.XPN.TRFT.ZS (social transfers % expense, time series)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class WelfareStateMaturity(LayerBase):
    layer_id = "lWS"
    name = "Welfare State Maturity"

    async def compute(self, db, **kwargs) -> dict:
        code = "GC.XPN.TRFT.ZS"
        name = "social transfers"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no social transfers time series"}

        vals = [float(r["value"]) for r in rows if r["value"] is not None]
        if len(vals) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient time series data"}

        mean_val = float(np.mean(vals))
        std_val = float(np.std(vals))
        cv = std_val / mean_val if mean_val > 0 else 1.0  # coefficient of variation

        # Level component: higher spending = more mature (reduces score)
        level_component = min(60.0, (mean_val / 20.0) * 60.0)
        # Volatility component: lower cv = more stable = lower score
        volatility_component = min(40.0, cv * 40.0)

        score = float(np.clip(100.0 - level_component + volatility_component, 0, 100))

        return {
            "score": round(score, 1),
            "mean_transfers_pct": round(mean_val, 2),
            "std_transfers_pct": round(std_val, 2),
            "coefficient_of_variation": round(cv, 3),
            "observations": len(vals),
            "interpretation": (
                "nascent welfare state" if score > 75
                else "developing welfare state" if score > 50
                else "established welfare state" if score > 25
                else "mature welfare state"
            ),
            "sources": ["WDI GC.XPN.TRFT.ZS"],
        }
