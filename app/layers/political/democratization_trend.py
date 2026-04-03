"""Democratization Trend module.

Democratic consolidation: voice and accountability trend over 10 years.

Theory:
    Diamond (1999) and Huntington (1991) document that democracy consolidation
    requires sustained improvement in political participation and accountability
    over a decade or more. A declining trend in WGI VA.EST over 10 years signals
    democratic backsliding -- a structural reversal that differs from short-term
    political volatility. Lührmann & Lindberg (2019) identify backsliding as a
    gradual erosion rather than sudden breakdown, captured by trend slopes.

Indicator:
    - VA.EST: Voice and Accountability (WGI). Range -2.5 to 2.5. Higher = better.

Score construction:
    Fit linear regression on VA.EST over last 10 years.
    score = clip(50 - slope * 200, 0, 100)
    Declining democratization (negative slope) = high stress.
    Flat or improving trend = low stress.

References:
    Diamond, L. (1999). Developing Democracy. Johns Hopkins UP.
    Huntington, S. P. (1991). The Third Wave. Univ. of Oklahoma Press.
    Lührmann, A. & Lindberg, S. (2019). "A third wave of autocratization."
        Democratization 26(7).
    World Bank. (2023). Worldwide Governance Indicators.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class DemocratizationTrend(LayerBase):
    layer_id = "l12"
    name = "Democratization Trend"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate democratic consolidation via VA.EST trend.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 code (default BGD)
        """
        country = kwargs.get("country_iso3", "BGD")

        va_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%VA.EST%' OR ds.name LIKE '%voice%accountability%'
                   OR ds.name LIKE '%voice%and%accountability%')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not va_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no voice/accountability data"}

        # Restrict to last 10 years if enough data available
        rows_10y = va_rows[-10:] if len(va_rows) >= 10 else va_rows
        va = np.array([float(r["value"]) for r in rows_10y])
        va_latest = float(va[-1])

        if len(va) < 3:
            # Not enough data for trend, fall back to level-based score
            score = float(np.clip(50 - va_latest * 20, 0, 100))
            return {
                "score": round(score, 2),
                "country": country,
                "va_latest": round(va_latest, 4),
                "n_obs": len(va),
                "note": "Insufficient data for trend; score based on level.",
                "reference": "Diamond 1999; Lührmann & Lindberg 2019; WGI VA.EST",
            }

        t = np.arange(len(va), dtype=float)
        slope, intercept, r_val, p_val, se = stats.linregress(t, va)

        score = float(np.clip(50 - slope * 200, 0, 100))

        trend_status = "consolidating" if slope > 0.01 else "backsliding" if slope < -0.01 else "stagnant"

        result = {
            "score": round(score, 2),
            "country": country,
            "va_latest": round(va_latest, 4),
            "va_mean_10y": round(float(np.mean(va)), 4),
            "trend": {
                "slope_per_year": round(float(slope), 5),
                "direction": "improving" if slope > 0 else "declining",
                "r_squared": round(float(r_val ** 2), 4),
                "p_value": round(float(p_val), 4),
                "se": round(float(se), 6),
                "years_analyzed": len(va),
            },
            "democratization_status": trend_status,
            "backsliding_risk": (
                "high" if score > 65 else "moderate" if score > 35 else "low"
            ),
            "date_range": [str(rows_10y[0]["date"]), str(rows_10y[-1]["date"])],
            "reference": "Diamond 1999; Huntington 1991; Lührmann & Lindberg 2019; WGI VA.EST",
        }

        return result
