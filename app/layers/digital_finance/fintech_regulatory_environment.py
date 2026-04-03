"""Fintech Regulatory Environment module.

Regulatory quality + business environment for fintech:
  - RQ.EST: regulatory quality estimate (WGI, range roughly -2.5 to +2.5)
  - IC.BUS.EASE.XQ: ease of doing business score (WDI, 0-100, higher=better)

Low regulatory quality + high entry barriers = poor fintech environment => high score (stress).

Score maps (low RQ + low ease-of-business) to 0-100.

Source: World Bank WGI (RQ.EST), World Bank Doing Business (IC.BUS.EASE.XQ)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FintechRegulatoryEnvironment(LayerBase):
    layer_id = "lDF"
    name = "Fintech Regulatory Environment"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rq_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'RQ.EST'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        ease_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IC.BUS.EASE.XQ'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not rq_rows and not ease_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        rq_vals = [float(r["value"]) for r in rq_rows if r["value"] is not None]
        ease_vals = [float(r["value"]) for r in ease_rows if r["value"] is not None]

        rq_mean = float(np.nanmean(rq_vals)) if rq_vals else None
        ease_mean = float(np.nanmean(ease_vals)) if ease_vals else None

        # Normalize RQ from [-2.5, +2.5] to [0, 100] (higher = better)
        rq_norm = float(np.clip((rq_mean + 2.5) / 5.0 * 100, 0, 100)) if rq_mean is not None else 50.0
        # Ease of business is already 0-100
        ease_norm = float(np.clip(ease_mean or 0, 0, 100)) if ease_mean is not None else 50.0

        weights = []
        components = []
        if rq_mean is not None:
            weights.append(0.6)
            components.append(rq_norm)
        if ease_mean is not None:
            weights.append(0.4)
            components.append(ease_norm)

        total_w = sum(weights)
        env_quality = sum(c * w for c, w in zip(components, weights)) / total_w if total_w > 0 else 50.0
        score = float(np.clip(100.0 - env_quality, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "regulatory_quality_est": round(rq_mean, 4) if rq_mean is not None else None,
            "ease_of_business_score": round(ease_mean, 2) if ease_mean is not None else None,
            "rq_norm": round(rq_norm, 2),
            "ease_norm": round(ease_norm, 2),
            "env_quality_composite": round(env_quality, 2),
            "note": "Score 0 = excellent fintech environment. Score 100 = hostile.",
            "_citation": "World Bank WGI: RQ.EST; World Bank Doing Business: IC.BUS.EASE.XQ",
        }
