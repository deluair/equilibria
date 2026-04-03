"""Peacebuilding Returns module.

Estimates the economic returns to peace investment by examining the relationship
between governance improvement and economic growth dividends. Uses improvement
in political stability (PV.EST) and subsequent GDP growth (NY.GDP.MKTP.KD.ZG)
to compute peace dividend. Higher stability improvement correlated with stronger
growth signals positive peacebuilding returns.

Score = clip((1 - returns_index) * 100, 0, 100).
Low score = high returns to peace (peacebuilding is paying off).
High score = low returns (peacebuilding investment not translating to growth).

Sources: WDI (PV.EST, NY.GDP.MKTP.KD.ZG, NE.GDI.TOTL.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PeacebuildingReturns(LayerBase):
    layer_id = "lCW"
    name = "Peacebuilding Returns"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        stability_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'PV.EST'
            ORDER BY dp.date
            LIMIT 15
            """,
            (country,),
        )

        growth_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY dp.date
            LIMIT 15
            """,
            (country,),
        )

        inv_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.GDI.TOTL.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not stability_rows and not growth_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        stability_vals = [float(r["value"]) for r in stability_rows if r["value"] is not None]
        growth_vals = [float(r["value"]) for r in growth_rows if r["value"] is not None]

        if not stability_vals and not growth_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        # Stability trajectory: improvement = positive peacebuilding signal
        stability_trend = None
        stability_improvement = 0.0
        if len(stability_vals) >= 3:
            s_arr = np.array(stability_vals)
            t = np.arange(len(s_arr))
            coeffs = np.polyfit(t, s_arr, 1)
            stability_trend = float(coeffs[0])  # slope
            # Improvement = positive slope
            stability_improvement = max(stability_trend, 0.0)

        # Growth response to stability
        growth_mean = float(np.mean(growth_vals)) if growth_vals else None

        # Investment response (peace dividend boosts investment)
        inv_vals = [float(r["value"]) for r in inv_rows if r["value"] is not None]
        inv_mean = float(np.mean(inv_vals)) if inv_vals else None

        # Returns index: high stability improvement + growth = high returns
        if stability_improvement > 0 and growth_mean is not None and growth_mean > 0:
            returns_index = float(np.clip(stability_improvement * 0.5 + growth_mean * 0.05, 0, 1))
        elif growth_mean is not None:
            returns_index = float(np.clip(max(growth_mean, 0) * 0.02, 0, 0.5))
        else:
            returns_index = 0.0

        # Score inverted: high score = low returns (problem), low score = high returns (good)
        # But per spec: score measures "economic returns to peace investment" as indicator
        # We treat this similarly to other modules: high score = stress/concern (low returns)
        score = float(np.clip((1.0 - returns_index) * 100, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "stability_trend_slope": round(stability_trend, 6) if stability_trend is not None else None,
            "stability_improvement": round(stability_improvement, 6),
            "gdp_growth_mean": round(growth_mean, 4) if growth_mean is not None else None,
            "investment_rate_mean": round(inv_mean, 4) if inv_mean is not None else None,
            "returns_index": round(returns_index, 4),
            "interpretation": "lower score = higher peacebuilding returns (favorable)",
            "n_stability_obs": len(stability_vals),
            "n_growth_obs": len(growth_vals),
            "indicators": {
                "political_stability": "PV.EST",
                "gdp_growth": "NY.GDP.MKTP.KD.ZG",
                "investment_rate": "NE.GDI.TOTL.ZS",
            },
        }
