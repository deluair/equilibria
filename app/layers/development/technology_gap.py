"""Technology gap: distance from technology frontier.

Measures how far a country's technology adoption lags behind the high-income
frontier. Uses internet users (% of population) as a proxy for ICT adoption.
Benchmark: 80% (typical high-income saturation level).

Key references:
    Acemoglu, D., Aghion, P. & Zilibotti, F. (2006). Distance to frontier,
        selection, and economic growth. Journal of the European Economic
        Association, 4(1), 37-74.
    ITU (2023). Measuring digital development: Facts and figures.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

FRONTIER_BENCHMARK = 80.0  # % internet users in high-income economies


class TechnologyGap(LayerBase):
    layer_id = "l4"
    name = "Technology Gap"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Technology frontier distance via internet users vs 80% benchmark.

        Queries IT.NET.USER.ZS (individuals using the internet, % of population).
        Score = clip(max(0, 80 - latest_pct) * 1.25, 0, 100). Large gap = stress.

        Returns dict with score, latest internet usage rate, gap from frontier,
        and trend direction.
        """
        country_iso3 = kwargs.get("country_iso3")

        rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'IT.NET.USER.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not rows:
            return {"score": 50, "results": {"error": "no internet user data available"}}

        internet_data: dict[str, dict[str, float]] = {}
        for r in rows:
            internet_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        # Global distribution for context
        latest_vals = []
        for iso, years_data in internet_data.items():
            if years_data:
                latest_year = max(years_data.keys())
                latest_vals.append(years_data[latest_year])

        global_median = float(np.median(latest_vals)) if latest_vals else None

        # Target country
        target_analysis = None
        score = 50.0

        if country_iso3 and country_iso3 in internet_data:
            iso_data = internet_data[country_iso3]
            years = sorted(iso_data.keys())
            latest_pct = iso_data[years[-1]]
            gap = max(0.0, FRONTIER_BENCHMARK - latest_pct)
            raw_score = gap * 1.25

            # Trend: compare to 5 years ago if available
            trend = None
            if len(years) >= 5:
                old_pct = iso_data[years[-5]]
                change = latest_pct - old_pct
                trend = "improving" if change > 2 else "stagnant" if change >= 0 else "declining"

            target_analysis = {
                "latest_internet_pct": latest_pct,
                "frontier_benchmark": FRONTIER_BENCHMARK,
                "gap_from_frontier": gap,
                "trend_5yr": trend,
                "global_median": global_median,
                "above_global_median": latest_pct > global_median if global_median else None,
            }

            score = float(np.clip(raw_score, 0, 100))
        elif latest_vals:
            # Fallback: use global median gap
            gap = max(0.0, FRONTIER_BENCHMARK - (global_median or 0))
            score = float(np.clip(gap * 1.25, 0, 100))

        return {
            "score": score,
            "results": {
                "frontier_benchmark_pct": FRONTIER_BENCHMARK,
                "global_median_internet_pct": global_median,
                "n_countries": len(internet_data),
                "target": target_analysis,
                "country_iso3": country_iso3,
            },
        }
