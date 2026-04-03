"""Regional Inequality Index module.

Proxies urban-rural income divergence by examining whether urbanization has
been accompanied by rising income per capita. Rapid urbanization with stagnant
or slow income growth signals that urban areas are not distributing gains to
other regions -- workers move to cities but the income gap between regions
persists.

Approach:
  1. Query urbanization rate trend and GDP per capita trend over available years.
  2. Compute urbanization growth rate and income growth rate.
  3. If urbanization is growing faster than income, the income-urban premium is
     not spreading regionally: divergence_index = urban_growth - income_growth.

Score = clip(divergence_index * 5, 0, 100)
where divergence_index is annual percentage-point difference.

Sources: WDI SP.URB.TOTL.IN.ZS (urban population % of total),
         WDI NY.GDP.PCAP.KD (GDP per capita constant 2015 USD)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RegionalInequalityIndex(LayerBase):
    layer_id = "lRD"
    name = "Regional Inequality Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows_urban = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.URB.TOTL.IN.ZS'
            ORDER BY dp.date ASC
            LIMIT 20
            """,
            (country,),
        )

        rows_gdp = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY dp.date ASC
            LIMIT 20
            """,
            (country,),
        )

        if not rows_urban or not rows_gdp:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        urban_map = {r["date"]: float(r["value"]) for r in rows_urban if r["value"] is not None}
        gdp_map = {r["date"]: float(r["value"]) for r in rows_gdp if r["value"] is not None}

        common_dates = sorted(set(urban_map) & set(gdp_map))
        if len(common_dates) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient overlapping observations"}

        urban_vals = np.array([urban_map[d] for d in common_dates])
        gdp_vals = np.array([gdp_map[d] for d in common_dates])

        n = len(common_dates)
        x = np.arange(n, dtype=float)

        urban_trend = float(np.polyfit(x, urban_vals, 1)[0])   # ppt per year
        # Income growth: slope of log(gdp) per year
        log_gdp = np.log(np.maximum(gdp_vals, 1.0))
        income_trend = float(np.polyfit(x, log_gdp, 1)[0]) * 100  # % per year

        divergence_index = urban_trend - income_trend

        score = float(np.clip(divergence_index * 5, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "period": f"{common_dates[0]} to {common_dates[-1]}",
            "urban_trend_ppt_per_year": round(urban_trend, 3),
            "income_growth_pct_per_year": round(income_trend, 3),
            "divergence_index": round(divergence_index, 3),
            "n_obs": n,
            "series": {
                "urban": "SP.URB.TOTL.IN.ZS",
                "income": "NY.GDP.PCAP.KD",
            },
            "interpretation": (
                "urbanization faster than income growth = regions not sharing urban gains (divergence)"
                if divergence_index > 0
                else "income growing faster than urbanization (potential convergence)"
            ),
        }
