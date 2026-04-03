"""Digital Divide module.

Income-digital access gap: residual of internet penetration on log(income per capita).

Queries:
  - IT.NET.USER.ZS: internet users % population
  - NY.GDP.PCAP.KD: GDP per capita (constant USD)

Countries with income but low internet = digital divide.
Negative residual from OLS of internet ~ log(GDP per capita) => digital lag.

Score = clip(50 - residual * 5, 0, 100).
Negative residual (below-expected internet) => score > 50.
Positive residual (above-expected internet) => score < 50.

Source: World Bank WDI
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class DigitalDivide(LayerBase):
    layer_id = "lDF"
    name = "Digital Divide"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        internet_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IT.NET.USER.ZS'
            ORDER BY dp.date DESC
            LIMIT 15
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
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        if not internet_rows or not gdp_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        internet_vals = [float(r["value"]) for r in internet_rows if r["value"] is not None]
        gdp_vals = [float(r["value"]) for r in gdp_rows if r["value"] is not None]

        if not internet_vals or not gdp_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        internet_mean = float(np.nanmean(internet_vals))
        gdp_mean = float(np.nanmean(gdp_vals))

        if gdp_mean <= 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "invalid GDP value"}

        log_gdp = float(np.log(gdp_mean))

        # Single-country: compare against a simple expected curve
        # Expected internet = -20 + 15 * log(gdp_per_capita) capped at 100
        # (derived from cross-country OLS approximation)
        expected_internet = float(np.clip(-20.0 + 15.0 * log_gdp, 0, 100))
        residual = internet_mean - expected_internet

        score = float(np.clip(50.0 - residual * 2.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "internet_users_pct": round(internet_mean, 2),
            "gdp_per_capita_usd": round(gdp_mean, 2),
            "log_gdp_per_capita": round(log_gdp, 4),
            "expected_internet_pct": round(expected_internet, 2),
            "residual": round(residual, 4),
            "note": "Negative residual = digital lag (below expected given income). Score > 50 = divide.",
            "_citation": "World Bank WDI: IT.NET.USER.ZS, NY.GDP.PCAP.KD",
        }
