"""Crop production index trend analysis.

Tracks the trajectory of the FAO crop production index (2014-16 = 100)
over time. A declining index indicates that aggregate crop output (adjusted
for area and input prices) is falling relative to the base period, which
constitutes an agricultural sector stress signal.

Methodology:
    Fetch the crop production index time series (WDI: AG.PRD.CROP.XD) and
    apply OLS linear regression:

        index_t = alpha + beta * t + e_t

    A negative slope (beta < 0) represents declining crop production relative
    to the 2014-16 baseline. The stress score:

        score = clip(-slope * 5, 0, 100)

    Benchmarks:
        slope = 0: score = 0 (stable, no stress).
        slope = -20 index points/year: score = 100 (severe crisis).
        slope = +5: score = 0 (growing production, no stress).

Score (0-100): Higher score indicates greater crop production stress
(falling or stagnant production index).

References:
    World Bank WDI indicator AG.PRD.CROP.XD (FAO crop production index).
    FAO (2022). "FAOSTAT Production Domain." food.un.org.
    World Bank (2023). World Development Indicators.
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class CropProductionIndex(LayerBase):
    layer_id = "l5"
    name = "Crop Production Index"

    async def compute(self, db, **kwargs) -> dict:
        """Compute crop production index trend and stress score.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            min_obs : int - minimum observations required (default 5)
        """
        country = kwargs.get("country_iso3", "BGD")
        min_obs = kwargs.get("min_obs", 5)

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.indicator_code = 'AG.PRD.CROP.XD'
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        if not rows or len(rows) < min_obs:
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE '%crop%production%index%'
                ORDER BY dp.date ASC
                """,
                (country,),
            )

        if not rows or len(rows) < min_obs:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"insufficient crop production index data (need >= {min_obs} observations)",
            }

        years = []
        values = []
        for r in rows:
            if r["value"] is not None:
                try:
                    years.append(int(str(r["date"])[:4]))
                    values.append(float(r["value"]))
                except (ValueError, TypeError):
                    continue

        if len(years) < min_obs:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient valid crop production index observations",
            }

        t = np.array(years, dtype=float)
        y = np.array(values, dtype=float)

        result = linregress(t, y)
        slope = float(result.slope)
        intercept = float(result.intercept)
        r_squared = float(result.rvalue ** 2)
        p_value = float(result.pvalue)

        # Negative slope = declining production = rising stress
        score = float(np.clip(-slope * 5.0, 0.0, 100.0))

        trend_direction = (
            "declining" if slope < -0.5
            else "stagnant" if slope < 0.5
            else "growing"
        )

        return {
            "score": round(score, 2),
            "country": country,
            "crop_production_index_trend": {
                "slope_per_year": round(slope, 4),
                "intercept": round(intercept, 4),
                "r_squared": round(r_squared, 4),
                "p_value": round(p_value, 4),
                "trend_direction": trend_direction,
            },
            "latest_index_value": round(float(y[-1]), 2),
            "mean_index_value": round(float(y.mean()), 2),
            "base_period": "2014-16 = 100",
            "n_obs": len(y),
            "period": {"start": int(t[0]), "end": int(t[-1])},
            "indicator": "AG.PRD.CROP.XD",
        }
