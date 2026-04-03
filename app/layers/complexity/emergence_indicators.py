"""Emergence Indicators module.

Detects non-linear growth emergence: whether recent GDP growth significantly
deviates above the long-run trend (positive emergence = lower stress).

Score = clip(50 - recent_vs_trend * 10, 0, 100)
Positive deviation (faster than trend) reduces score. Negative raises it.

Sources: WDI NY.GDP.MKTP.KD.ZG (GDP growth, annual %)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class EmergenceIndicators(LayerBase):
    layer_id = "lCP"
    name = "Emergence Indicators"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]

        long_run_mean = float(np.mean(values))
        long_run_std = float(np.std(values)) if len(values) > 1 else 1.0

        # Recent = last 3 years
        recent = values[-3:]
        recent_mean = float(np.mean(recent))

        # Deviation above trend = emergence (positive = good = lower stress)
        recent_vs_trend = recent_mean - long_run_mean

        # Normalize by std so units are in sigma
        if long_run_std > 0:
            recent_vs_trend_z = recent_vs_trend / long_run_std
        else:
            recent_vs_trend_z = 0.0

        score = float(np.clip(50.0 - recent_vs_trend_z * 10.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "long_run_mean_growth_pct": round(long_run_mean, 3),
            "long_run_std_pct": round(long_run_std, 3),
            "recent_3yr_mean_growth_pct": round(recent_mean, 3),
            "recent_vs_trend_z": round(recent_vs_trend_z, 4),
            "period": f"{dates[0]} to {dates[-1]}",
            "n_obs": len(values),
            "interpretation": (
                "High score = recent growth below long-run trend (no emergence). "
                "Low score = recent growth well above trend (emergence signal)."
            ),
            "_citation": "World Bank WDI: NY.GDP.MKTP.KD.ZG",
        }
