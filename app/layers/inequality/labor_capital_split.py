"""Labor-Capital Split module.

Measures the functional income distribution: divergence between labor
productivity (output per worker) and aggregate income (GDP per capita).

When output per worker grows faster than GDP per capita, it signals that
productivity gains are flowing disproportionately to capital rather than
to workers (widening capital-labor split in income distribution).

Indicators:
- SL.GDP.PCAP.EM.KD: GDP per person employed (constant 2017 PPP USD)
  (proxy for labor productivity / output per worker)
- NY.GDP.PCAP.KD: GDP per capita (constant 2015 USD)

Method:
- Compute growth rates of both series over available years.
- Divergence = productivity_growth - gdp_pc_growth.
  Positive divergence: productivity rising faster than per-capita income
  (workers not fully capturing gains).
- Score = clip(divergence_annualized * 5 + level_gap_score, 0, 100).

Level gap: productivity / gdp_pc ratio above a threshold also signals
capital concentration (workers produce more than they receive in per-capita
terms).

Sources: World Bank WDI (SL.GDP.PCAP.EM.KD, NY.GDP.PCAP.KD)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class LaborCapitalSplit(LayerBase):
    layer_id = "lIQ"
    name = "Labor Capital Split"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        prod_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.GDP.PCAP.EM.KD'
            ORDER BY dp.date
            """,
            (country,),
        )

        gdp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not prod_rows or not gdp_rows or len(prod_rows) < 3 or len(gdp_rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        prod_values = np.array([float(r["value"]) for r in prod_rows])
        prod_years = np.array([float(r["date"][:4]) for r in prod_rows])
        gdp_values = np.array([float(r["value"]) for r in gdp_rows])
        gdp_years = np.array([float(r["date"][:4]) for r in gdp_rows])

        # Log-linear growth rates
        prod_slope, *_ = linregress(prod_years, np.log(prod_values + 1))
        gdp_slope, *_ = linregress(gdp_years, np.log(gdp_values + 1))

        divergence_annualized = float(prod_slope - gdp_slope)

        # Level gap: ratio of latest output per worker to gdp per capita
        prod_latest = float(prod_values[-1])
        gdp_latest = float(gdp_values[-1])
        level_ratio = prod_latest / max(gdp_latest, 1.0)
        # level_ratio > 1 is typical (employment rate < 1 mechanically inflates ratio)
        # Extreme ratios signal labor concentration
        level_gap_score = float(np.clip((level_ratio - 2.0) * 5.0, 0, 30))

        # Divergence score: positive divergence = capital gaining faster
        divergence_score = float(np.clip(divergence_annualized * 100.0 * 5.0, 0, 70))

        score = float(np.clip(divergence_score + level_gap_score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs_productivity": len(prod_rows),
            "n_obs_gdp_pc": len(gdp_rows),
            "output_per_worker_latest": round(prod_latest, 0),
            "gdp_per_capita_latest": round(gdp_latest, 0),
            "level_ratio": round(level_ratio, 4),
            "productivity_growth_log_slope": round(float(prod_slope), 6),
            "gdp_pc_growth_log_slope": round(float(gdp_slope), 6),
            "divergence_annualized": round(divergence_annualized, 6),
            "interpretation": {
                "productivity_outpacing_income": divergence_annualized > 0,
                "capital_gaining": divergence_annualized > 0.005,
            },
        }
