"""Agricultural land productivity: cereal yield trend analysis.

Measures the long-run trajectory of cereal yields (kg/hectare) as a proxy
for agricultural land productivity. A declining trend signals land degradation,
soil exhaustion, or inadequate input use, and represents a key agricultural
stress indicator.

Methodology:
    Fetch annual cereal yield data (WDI indicator AG.YLD.CREL.KG) and apply
    ordinary least-squares linear regression over the available time series:

        yield_t = alpha + beta * t + e_t

    A negative slope (beta < 0) indicates declining productivity. The stress
    score is derived from the slope scaled to a 0-100 index:

        score = clip(50 - slope / 50, 0, 100)

    At slope = 0: score = 50 (neutral).
    At slope = -2500 kg/ha/yr: score = 100 (maximum stress).
    At slope = +2500 kg/ha/yr: score = 0 (no stress).

Score (0-100): Higher score indicates greater land productivity stress
(declining or stagnant cereal yields).

References:
    World Bank WDI indicator AG.YLD.CREL.KG.
    FAO (2021). "The State of Food and Agriculture."
    Lobell, D.B. et al. (2011). "Closing yield gaps through nutrient and
        water management." Nature, 478, 390-392.
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class AgriculturalLandProductivity(LayerBase):
    layer_id = "l5"
    name = "Agricultural Land Productivity"

    async def compute(self, db, **kwargs) -> dict:
        """Compute cereal yield trend and land productivity stress score.

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
              AND ds.indicator_code = 'AG.YLD.CREL.KG'
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        if not rows or len(rows) < min_obs:
            # Fallback: try name-based lookup
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND (ds.name LIKE '%cereal%yield%' OR ds.name LIKE '%AG.YLD.CREL%')
                ORDER BY dp.date ASC
                """,
                (country,),
            )

        if not rows or len(rows) < min_obs:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"insufficient cereal yield data (need >= {min_obs} observations)",
            }

        dates = []
        yields = []
        for r in rows:
            if r["value"] is not None and r["value"] > 0:
                try:
                    year = int(str(r["date"])[:4])
                    dates.append(year)
                    yields.append(float(r["value"]))
                except (ValueError, TypeError):
                    continue

        if len(dates) < min_obs:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient valid cereal yield observations",
            }

        t = np.array(dates, dtype=float)
        y = np.array(yields, dtype=float)

        result = linregress(t, y)
        slope = float(result.slope)
        intercept = float(result.intercept)
        r_squared = float(result.rvalue ** 2)
        p_value = float(result.pvalue)

        # score = clip(50 - slope/50, 0, 100)
        score = float(np.clip(50.0 - slope / 50.0, 0.0, 100.0))

        trend_direction = (
            "declining" if slope < -10
            else "stagnant" if slope < 10
            else "improving"
        )

        return {
            "score": round(score, 2),
            "country": country,
            "cereal_yield_trend": {
                "slope_kg_ha_per_year": round(slope, 4),
                "intercept": round(intercept, 2),
                "r_squared": round(r_squared, 4),
                "p_value": round(p_value, 4),
                "trend_direction": trend_direction,
            },
            "latest_yield_kg_ha": round(float(y[-1]), 2),
            "mean_yield_kg_ha": round(float(y.mean()), 2),
            "n_obs": len(y),
            "period": {"start": int(t[0]), "end": int(t[-1])},
            "indicator": "AG.YLD.CREL.KG",
        }
