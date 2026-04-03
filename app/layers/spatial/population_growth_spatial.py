"""Population growth rate and spatial carrying capacity stress.

Rapid population growth strains spatial resources: land, infrastructure,
housing, and public services. Above 2.5%/year signals demographic pressure
on spatial systems. Growth above 1.0%/year begins to generate measurable stress.

Score = clip(max(0, growth_rate - 1.0) * 25, 0, 100)
At 1.0%: score = 0; at 5.0%: score = 100.

References:
    Malthus, T.R. (1798). An Essay on the Principle of Population.
    Boserup, E. (1965). The Conditions of Agricultural Growth. Aldine.
    Cohen, J.E. (1995). How Many People Can the Earth Support? Norton.

Sources: World Bank WDI SP.POP.GROW.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PopulationGrowthSpatial(LayerBase):
    layer_id = "l11"
    name = "Population Growth Spatial"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.POP.GROW'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no population growth data",
                "country": country,
            }

        latest = rows[0]
        growth_rate = float(latest["value"])
        year = latest["date"]

        score = float(np.clip(max(0.0, growth_rate - 1.0) * 25.0, 0.0, 100.0))

        # 5-year average for smoothing
        recent_vals = [float(r["value"]) for r in rows[:5]]
        avg_5yr = float(np.mean(recent_vals))

        # Long-run trend
        trend_slope = None
        if len(rows) >= 5:
            vals = np.array([float(r["value"]) for r in reversed(rows)])
            t = np.arange(len(vals), dtype=float)
            trend_slope = round(float(np.polyfit(t, vals, 1)[0]), 5)

        return {
            "score": round(score, 2),
            "country": country,
            "growth_rate_pct": round(growth_rate, 3),
            "avg_5yr_pct": round(avg_5yr, 3),
            "year": year,
            "trend_slope_pp_per_yr": trend_slope,
            "pressure_level": (
                "critical" if growth_rate > 2.5
                else "high" if growth_rate > 2.0
                else "moderate" if growth_rate > 1.0
                else "low"
            ),
            "n_obs": len(rows),
            "_source": "WDI SP.POP.GROW",
        }
