"""Monetary Base Growth: reserve money growth vs nominal GDP growth, inflation risk.

Methodology
-----------
Friedman (1963) monetarist proposition: sustained excess money growth above
real output growth produces inflation with a long and variable lag.

    excess_growth = money_growth - nominal_gdp_growth

where:
    money_growth       = annual % change in broad money (M2/GDP proxy YoY)
    nominal_gdp_growth = real GDP growth + inflation (approximated from WDI data)

Threshold: excess_growth > 10 pp -> elevated inflation risk.

The score reflects the 3-year rolling average of excess money growth:
    score = clip(max(0, avg_excess) * 5, 0, 100)

Sources: World Bank WDI
  FM.LBL.BMNY.GD.ZS  - Broad money (% of GDP) used as proxy for M2 level
  NY.GDP.MKTP.KD.ZG   - GDP growth (constant prices, %)
  FP.CPI.TOTL.ZG      - Inflation, consumer prices (annual %)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MonetaryBaseGrowth(LayerBase):
    layer_id = "l15"
    name = "Monetary Base Growth"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 20)
        excess_threshold = kwargs.get("excess_threshold_pct", 10.0)

        series_map = {
            "broad_money_gdp": f"FM.LBL.BMNY.GD.ZS_{country}",
            "gdp_growth": f"NY.GDP.MKTP.KD.ZG_{country}",
            "inflation": f"FP.CPI.TOTL.ZG_{country}",
        }

        data: dict[str, dict[str, float]] = {}
        for label, code in series_map.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE series_id = ?) "
                "AND date >= date('now', ?) ORDER BY date",
                (code, f"-{lookback} years"),
            )
            if rows:
                data[label] = {r[0]: float(r[1]) for r in rows}

        if not data.get("broad_money_gdp"):
            return {"score": 50.0, "results": {"error": "insufficient broad money data"}}

        bm_dates = sorted(data["broad_money_gdp"])
        bm_vals = np.array([data["broad_money_gdp"][d] for d in bm_dates])

        # Annual % change in broad money ratio as proxy for M2 growth
        if len(bm_vals) < 3:
            return {"score": 50.0, "results": {"error": "too few observations for growth calculation"}}

        money_growth = np.diff(bm_vals) / np.maximum(np.abs(bm_vals[:-1]), 1e-6) * 100.0
        growth_dates = bm_dates[1:]

        # Nominal GDP growth = real growth + inflation
        nominal_gdp_growth: dict[str, float] = {}
        if data.get("gdp_growth") and data.get("inflation"):
            for d in growth_dates:
                rg = data["gdp_growth"].get(d)
                inf = data["inflation"].get(d)
                if rg is not None and inf is not None:
                    nominal_gdp_growth[d] = rg + inf
        elif data.get("gdp_growth"):
            for d in growth_dates:
                v = data["gdp_growth"].get(d)
                if v is not None:
                    nominal_gdp_growth[d] = v

        # Excess growth where nominal GDP is available
        excess_series: list[float] = []
        matched_dates: list[str] = []
        for i, d in enumerate(growth_dates):
            if d in nominal_gdp_growth:
                exc = float(money_growth[i]) - nominal_gdp_growth[d]
                excess_series.append(exc)
                matched_dates.append(d)

        results: dict = {
            "country": country,
            "n_obs_money_growth": len(money_growth),
            "period": f"{bm_dates[0]} to {bm_dates[-1]}",
            "money_growth_latest_pct": round(float(money_growth[-1]), 3),
            "money_growth_mean_pct": round(float(np.mean(money_growth)), 3),
            "excess_threshold_pct": excess_threshold,
        }

        if excess_series:
            exc_arr = np.array(excess_series)
            rolling_avg = float(np.mean(exc_arr[-3:])) if len(exc_arr) >= 3 else float(np.mean(exc_arr))
            results["excess_growth_latest_pp"] = round(float(exc_arr[-1]), 3)
            results["excess_growth_mean_pp"] = round(float(np.mean(exc_arr)), 3)
            results["excess_growth_3yr_avg_pp"] = round(rolling_avg, 3)
            results["inflation_risk_flagged"] = rolling_avg > excess_threshold
        else:
            rolling_avg = float(np.mean(money_growth[-3:])) if len(money_growth) >= 3 else float(np.mean(money_growth))
            results["note"] = "no nominal GDP growth data; using money growth alone"
            results["excess_growth_3yr_avg_pp"] = round(rolling_avg, 3)

        score = float(np.clip(max(0.0, rolling_avg) * 5.0, 0.0, 100.0))

        return {"score": round(score, 1), "results": results}
