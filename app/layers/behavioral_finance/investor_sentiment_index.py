"""Investor Sentiment Index module.

Private credit growth cycle as a sentiment proxy. Rapid expansion in private
credit relative to GDP signals overly optimistic investor sentiment, often
preceding financial stress.

Sources: WDI FS.AST.PRVT.GD.ZS (domestic credit to private sector % of GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class InvestorSentimentIndex(LayerBase):
    layer_id = "lBF"
    name = "Investor Sentiment Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("FS.AST.PRVT.GD.ZS", "%private sector%credit%"),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        vals = np.array([float(r["value"]) for r in rows])
        # Reverse to chronological order (DESC fetch)
        vals = vals[::-1]

        changes = np.diff(vals)
        mean_change = float(np.mean(changes))
        max_change = float(np.max(changes))
        credit_level = float(vals[-1])

        # High credit level + fast growth = euphoric sentiment
        level_score = np.clip((credit_level - 50) / 1.5, 0, 50)
        growth_score = np.clip(max(0.0, mean_change) * 3, 0, 50)
        score = float(level_score + growth_score)

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(rows),
            "current_private_credit_pct_gdp": round(credit_level, 2),
            "mean_annual_change": round(mean_change, 3),
            "max_single_year_change": round(max_change, 3),
            "interpretation": "Rapid private credit expansion signals excess investor optimism",
        }
