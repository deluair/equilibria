"""Anticorruption Effectiveness module.

Corruption control trend analysis using WGI CC.EST over time.

Theory:
    Corruption undermines state capacity, distorts resource allocation, and
    reduces investment. Kaufmann et al. (2010) show WGI CC.EST captures
    perceptions of the extent to which public power is exercised for private
    gain. A declining trend in corruption control signals deteriorating
    institutional quality and increases economic stress.

Indicator:
    - CC.EST: Control of Corruption (WGI). Range -2.5 to 2.5. Higher = better.

Score construction:
    level_component = clip(0.5 - cc_latest * 0.2, 0, 1)  [stress from level]
    trend_component = clip(-slope * 20, 0, 1)  [stress from declining trend]
    score = clip((level_component * 0.6 + trend_component * 0.4) * 100, 0, 100)
    Declining control of corruption = high stress.

References:
    Kaufmann, D., Kraay, A. & Mastruzzi, M. (2010). "The Worldwide Governance
        Indicators: A Summary of Methodology." World Bank Policy Research WP 5430.
    Mauro, P. (1995). "Corruption and Growth." QJE 110(3).
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class AnticorruptionEffectiveness(LayerBase):
    layer_id = "l12"
    name = "Anticorruption Effectiveness"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate corruption control trend.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 code (default BGD)
        """
        country = kwargs.get("country_iso3", "BGD")

        cc_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%CC.EST%' OR ds.name LIKE '%control%corruption%estimate%'
                   OR ds.name LIKE '%control%of%corruption%')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not cc_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no corruption control data"}

        cc = np.array([float(r["value"]) for r in cc_rows])
        cc_latest = float(cc[-1])

        level_component = float(np.clip(0.5 - cc_latest * 0.2, 0, 1))

        trend = None
        trend_component = 0.5
        if len(cc) >= 3:
            t = np.arange(len(cc), dtype=float)
            slope, intercept, r_val, p_val, se = stats.linregress(t, cc)
            trend_component = float(np.clip(-slope * 20, 0, 1))
            trend = {
                "slope_per_year": round(float(slope), 5),
                "direction": "improving" if slope > 0 else "declining",
                "r_squared": round(float(r_val ** 2), 4),
                "p_value": round(float(p_val), 4),
                "years_observed": len(cc),
            }

        score = float(np.clip(
            (level_component * 0.6 + trend_component * 0.4) * 100,
            0, 100,
        ))

        result = {
            "score": round(score, 2),
            "country": country,
            "cc_latest": round(cc_latest, 4),
            "cc_mean": round(float(np.mean(cc)), 4),
            "score_components": {
                "level_stress": round(level_component * 0.6 * 100, 2),
                "trend_stress": round(trend_component * 0.4 * 100, 2),
            },
            "anticorruption_tier": (
                "weak" if score > 65 else "moderate" if score > 35 else "strong"
            ),
            "n_obs": len(cc),
            "date_range": [str(cc_rows[0]["date"]), str(cc_rows[-1]["date"])],
            "reference": "Kaufmann et al. 2010; Mauro 1995; WGI CC.EST",
        }

        if trend:
            result["trend"] = trend

        return result
