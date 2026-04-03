"""Reserve Money Growth: reserve money growth vs nominal GDP growth.

Methodology
-----------
Reserve money (monetary base) growth in excess of nominal GDP growth is
inflationary over time (quantity theory of money). This module measures
the excess reserve money growth as a monetary policy stress indicator.

Fischer (1993), Friedman & Schwartz (1963):
  excess_growth = reserve_money_growth - nominal_gdp_growth
  Sustained excess > 5pp signals monetary expansion beyond real economy needs

Score = clip(max(0, excess_growth) * 5, 0, 100)
  excess_growth = 0   -> score 0 (STABLE: in line with economy)
  excess_growth = 10% -> score 50 (WATCH)
  excess_growth = 20% -> score 100 (CRISIS)

Sources: WDI FM.LBL.BMNY.GD.ZS (broad money / GDP),
         NY.GDP.MKTP.KD.ZG (real GDP growth), FP.CPI.TOTL.ZG (inflation)
         to compute nominal GDP growth = real + inflation
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ReserveMoneyGrowth(LayerBase):
    layer_id = "lMY"
    name = "Reserve Money Growth"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        lookback = kwargs.get("lookback_years", 10)

        money_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FM.LBL.BMNY.GD.ZS'
              AND dp.date >= date('now', ?)
            ORDER BY dp.date
            """,
            (country, f"-{lookback} years"),
        )

        gdp_growth_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.MKTP.KD.ZG'
              AND dp.date >= date('now', ?)
            ORDER BY dp.date
            """,
            (country, f"-{lookback} years"),
        )

        inflation_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FP.CPI.TOTL.ZG'
              AND dp.date >= date('now', ?)
            ORDER BY dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not money_rows or len(money_rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient money supply data"}

        money_map = {r["date"]: float(r["value"]) for r in money_rows}
        money_arr = np.array([money_map[d] for d in sorted(money_map)])
        money_dates = sorted(money_map)

        # Money growth = % change in money / GDP ratio
        money_growth = float(np.mean(np.diff(money_arr) / (np.abs(money_arr[:-1]) + 1e-10) * 100))

        # Nominal GDP growth = real + inflation
        nom_gdp_growth: float | None = None
        if gdp_growth_rows and inflation_rows:
            gdp_map = {r["date"]: float(r["value"]) for r in gdp_growth_rows}
            inf_map = {r["date"]: float(r["value"]) for r in inflation_rows}
            common = sorted(set(gdp_map) & set(inf_map))
            if common:
                real_growth = float(np.mean([gdp_map[d] for d in common[-5:]]))
                inflation_avg = float(np.mean([inf_map[d] for d in common[-5:]]))
                nom_gdp_growth = real_growth + inflation_avg

        excess_growth = money_growth - nom_gdp_growth if nom_gdp_growth is not None else money_growth

        score = float(np.clip(max(0.0, excess_growth) * 5.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "money_growth_pct": round(money_growth, 2),
            "nominal_gdp_growth_pct": round(nom_gdp_growth, 2) if nom_gdp_growth is not None else None,
            "excess_growth_pp": round(excess_growth, 2),
            "excess_monetary_expansion": excess_growth > 0,
            "money_to_gdp_latest_pct": round(float(money_arr[-1]), 2),
            "n_obs": len(money_rows),
            "period": f"{money_dates[0]} to {money_dates[-1]}",
            "indicators": ["FM.LBL.BMNY.GD.ZS", "NY.GDP.MKTP.KD.ZG", "FP.CPI.TOTL.ZG"],
        }
