"""Anchoring Inflation module.

Inflation persistence as a proxy for anchoring failure. High autocorrelation
in CPI inflation implies agents anchor to past inflation rather than
forward-looking expectations, signaling weak credibility of monetary policy.

Sources: WDI FP.CPI.TOTL.ZG (CPI inflation annual %)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class AnchoringInflation(LayerBase):
    layer_id = "lBF"
    name = "Anchoring Inflation"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("FP.CPI.TOTL.ZG", "%CPI inflation%"),
        )

        if not rows or len(rows) < 6:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        vals = np.array([float(r["value"]) for r in rows])
        vals = vals[::-1]  # chronological order

        # AR(1) autocorrelation as persistence measure
        if len(vals) >= 2:
            corr_matrix = np.corrcoef(vals[:-1], vals[1:])
            autocorr = float(corr_matrix[0, 1])
        else:
            autocorr = 0.0

        mean_inflation = float(np.mean(vals))
        std_inflation = float(np.std(vals))

        # High autocorrelation = anchoring failure; also penalize high mean
        persistence_score = np.clip(max(0.0, autocorr) * 60, 0, 60)
        level_score = np.clip(max(0.0, mean_inflation - 2) * 2, 0, 40)
        score = float(persistence_score + level_score)

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(rows),
            "inflation_ar1_autocorr": round(autocorr, 3),
            "mean_inflation": round(mean_inflation, 3),
            "std_inflation": round(std_inflation, 3),
            "interpretation": "High AR(1) autocorrelation signals anchoring failure and backward-looking expectations",
        }
