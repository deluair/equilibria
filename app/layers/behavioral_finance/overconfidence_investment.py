"""Overconfidence Investment module.

Private investment boom and volatility as a signal of overconfidence.
Gross capital formation consistently outpacing or highly volatile relative
to historical norms indicates overconfident return expectations.

Sources: WDI NE.GDI.TOTL.ZS (gross capital formation % of GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class OverconfidenceInvestment(LayerBase):
    layer_id = "lBF"
    name = "Overconfidence Investment"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("NE.GDI.TOTL.ZS", "%gross capital formation%"),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        vals = np.array([float(r["value"]) for r in rows])
        vals = vals[::-1]  # chronological order

        mean_invest = float(np.mean(vals))
        std_invest = float(np.std(vals))
        changes = np.diff(vals)
        mean_change = float(np.mean(changes))
        max_change = float(np.max(np.abs(changes)))

        # High level + high volatility = overconfidence signal
        level_score = np.clip(max(0.0, mean_invest - 20) * 2.5, 0, 50)
        vol_score = np.clip(std_invest * 4, 0, 50)
        score = float(level_score + vol_score)

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(rows),
            "mean_investment_pct_gdp": round(mean_invest, 2),
            "std_investment": round(std_invest, 3),
            "mean_annual_change": round(mean_change, 3),
            "max_abs_change": round(max_change, 3),
            "interpretation": "High and volatile investment rate signals overconfident capital allocation",
        }
