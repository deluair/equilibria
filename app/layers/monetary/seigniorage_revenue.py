"""Seigniorage Revenue: inflation tax and fiscal-monetary dominance assessment.

Methodology
-----------
Seigniorage is the revenue accruing to the monetary authority from money creation.
In a closed economy with stable velocity:

    seigniorage/GDP = (delta_M / M) * (M/GDP) = money_growth_rate * (M2/GDP)

The inflation tax approximation (Cagan, 1956):

    inflation_tax/GDP = inflation_rate * (M2/GDP)

where the money holder pays the implicit tax through erosion of real balances.

High seigniorage signals fiscal-monetary dominance (Sargent & Wallace, 1981):
  - Government finances deficits via money creation
  - Central bank independence compromised
  - Self-reinforcing: higher inflation -> higher required seigniorage

Threshold: seigniorage > 2% of GDP is historically associated with high inflation
regimes and monetary instability (Fischer, Sahay & Vegh, 2002).

Score = clip(seigniorage_pct_gdp * 10, 0, 100)
  1% GDP  -> score 10 (watch)
  5% GDP  -> score 50 (stress)
  10% GDP -> score 100 (crisis)

Sources: World Bank WDI
  FP.CPI.TOTL.ZG   - Inflation, consumer prices (annual %)
  FM.LBL.BMNY.GD.ZS - Broad money (% of GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SeigniorageRevenue(LayerBase):
    layer_id = "l15"
    name = "Seigniorage Revenue"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 20)

        series_map = {
            "inflation": f"FP.CPI.TOTL.ZG_{country}",
            "broad_money_gdp": f"FM.LBL.BMNY.GD.ZS_{country}",
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

        if not data.get("inflation") or not data.get("broad_money_gdp"):
            return {"score": 50.0, "results": {"error": "insufficient inflation or broad money data"}}

        common = sorted(set(data["inflation"]) & set(data["broad_money_gdp"]))
        if len(common) < 3:
            return {"score": 50.0, "results": {"error": "too few overlapping observations"}}

        inf_arr = np.array([data["inflation"][d] for d in common]) / 100.0
        m2_arr = np.array([data["broad_money_gdp"][d] for d in common]) / 100.0

        # Inflation tax = inflation_rate * M2/GDP
        seigniorage = inf_arr * m2_arr * 100.0  # in % of GDP

        current = float(seigniorage[-1])
        mean_seig = float(np.mean(seigniorage))
        max_seig = float(np.max(seigniorage))

        # Trend in seigniorage
        t = np.arange(len(seigniorage), dtype=float)
        trend_slope = float(np.polyfit(t, seigniorage, 1)[0])

        # Fiscal dominance flag: persistently high (>= 3 consecutive years above 2%)
        above_threshold = seigniorage > 2.0
        consecutive = 0
        max_consecutive = 0
        for flag in above_threshold:
            if flag:
                consecutive += 1
                max_consecutive = max(max_consecutive, consecutive)
            else:
                consecutive = 0

        results: dict = {
            "country": country,
            "n_obs": len(common),
            "period": f"{common[0]} to {common[-1]}",
            "seigniorage_latest_pct_gdp": round(current, 4),
            "seigniorage_mean_pct_gdp": round(mean_seig, 4),
            "seigniorage_max_pct_gdp": round(max_seig, 4),
            "trend_slope_pct_gdp_yr": round(trend_slope, 5),
            "trend_increasing": trend_slope > 0,
            "max_consecutive_yrs_above_2pct": max_consecutive,
            "fiscal_monetary_dominance": max_consecutive >= 3,
            "severity": (
                "severe" if current > 5.0
                else "elevated" if current > 2.0
                else "low"
            ),
        }

        # Score per spec: clip(seigniorage * 10, 0, 100)
        score = float(np.clip(current * 10.0, 0.0, 100.0))
        if max_consecutive >= 3:
            score = min(score + 10.0, 100.0)

        return {"score": round(score, 1), "results": results}
