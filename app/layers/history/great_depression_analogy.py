"""Great Depression Analogy module.

Detects crisis patterns analogous to the Great Depression: simultaneous
GDP contraction and trade collapse. Co-occurrence of negative GDP growth
and a falling trade-to-GDP ratio in the same year signals a systemic
demand crisis with external-sector feedback.

Indicators:
  NY.GDP.MKTP.KD.ZG - GDP growth (annual %, WDI)
  NE.TRD.GNFS.ZS    - Trade (% of GDP, WDI)

Method: Count years where both GDP growth < 0 and trade share declined YoY.
Score based on frequency of co-negative years relative to total observed years.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class GreatDepressionAnalogy(LayerBase):
    layer_id = "lHI"
    name = "Great Depression Analogy"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        gdp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        trade_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.TRD.GNFS.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not gdp_rows or not trade_rows or len(gdp_rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        gdp_by_year = {r["date"][:4]: float(r["value"]) for r in gdp_rows}
        trade_by_year = {r["date"][:4]: float(r["value"]) for r in trade_rows}

        # Build aligned year list
        common_years = sorted(set(gdp_by_year) & set(trade_by_year))
        if len(common_years) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient overlapping years"}

        trade_vals = [trade_by_year[y] for y in common_years]
        trade_changes = np.diff(trade_vals)  # year-over-year change

        co_negative_count = 0
        for i, year in enumerate(common_years[1:]):
            gdp_growth = gdp_by_year.get(year, 0.0)
            trade_delta = float(trade_changes[i])
            if gdp_growth < 0 and trade_delta < 0:
                co_negative_count += 1

        n_periods = len(common_years) - 1
        frequency = co_negative_count / n_periods if n_periods > 0 else 0.0

        # Score: frequency of 0% -> 0, frequency of 20%+ -> 100
        score = float(np.clip(frequency * 500, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_years": len(common_years),
            "co_negative_years": co_negative_count,
            "frequency": round(frequency, 4),
            "period": f"{common_years[0]} to {common_years[-1]}",
            "latest_gdp_growth": round(gdp_by_year.get(common_years[-1], float("nan")), 3),
            "latest_trade_pct_gdp": round(trade_by_year.get(common_years[-1], float("nan")), 3),
        }
