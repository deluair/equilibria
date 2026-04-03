"""Living Standards module.

Assesses living standards via real GDP per capita growth trend and poverty
reduction trajectory. Declining real income + rising poverty = deteriorating
living standards and welfare stress.

Indicators:
  - NY.GDP.PCAP.KD.ZG : real GDP per capita growth (%)
  - SI.POV.DDAY       : poverty headcount at $2.15/day (%)

Sources: WDI
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class LivingStandards(LayerBase):
    layer_id = "lWE"
    name = "Living Standards"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        growth_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        poverty_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.POV.DDAY'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not growth_rows and not poverty_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no living standards data available",
            }

        score_components: dict[str, float] = {}
        result: dict = {"country": country}

        # Income component: mean growth and trend
        if growth_rows:
            growth_vals = np.array([float(r["value"]) for r in growth_rows])
            mean_growth = float(np.mean(growth_vals))
            result["mean_gdppc_growth_pct"] = round(mean_growth, 2)
            result["n_growth_obs"] = len(growth_rows)

            # Trend via linear regression
            if len(growth_rows) >= 3:
                x = np.arange(len(growth_vals))
                slope, _, _, _, _ = stats.linregress(x, growth_vals)
                result["growth_trend_slope"] = round(float(slope), 4)
            else:
                slope = 0.0
                result["growth_trend_slope"] = None

            # Penalty: low or declining mean growth
            income_penalty = float(np.clip((2.0 - mean_growth) * 6.0, 0, 50))
            # Additional penalty for declining trend
            trend_penalty = float(np.clip(-slope * 10.0, 0, 20))
            score_components["income"] = income_penalty + trend_penalty

        # Poverty component
        if poverty_rows:
            poverty_vals = np.array([float(r["value"]) for r in poverty_rows])
            poverty_latest = float(poverty_vals[-1])
            result["poverty_headcount_pct"] = round(poverty_latest, 2)
            result["n_poverty_obs"] = len(poverty_rows)

            if len(poverty_rows) >= 3:
                x = np.arange(len(poverty_vals))
                slope_p, _, _, _, _ = stats.linregress(x, poverty_vals)
                result["poverty_trend_slope"] = round(float(slope_p), 4)
            else:
                slope_p = 0.0
                result["poverty_trend_slope"] = None

            # Penalty: high poverty level + rising trend
            poverty_level_penalty = float(np.clip(poverty_latest * 0.4, 0, 40))
            rising_penalty = float(np.clip(slope_p * 5.0, 0, 20) if poverty_rows else 0)
            score_components["poverty"] = poverty_level_penalty + rising_penalty

        if not score_components:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data for living standards score",
            }

        score = float(np.clip(np.mean(list(score_components.values())), 0, 100))

        result.update({
            "score": round(score, 1),
            "score_components": {k: round(v, 2) for k, v in score_components.items()},
            "method": "Penalty composite: GDP per capita growth trend + poverty level and trend",
            "reference": "Ravallion 2001; Chen & Ravallion 2010; World Bank WDI",
        })

        return result
