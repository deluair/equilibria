"""Fiscal Governance module.

Measures fiscal responsibility using two indicators:
  GC.BAL.CASH.GD.ZS  Cash surplus/deficit as % of GDP (budget balance)
  GC.DOD.TOTL.GD.ZS  Central government debt, total (% of GDP)

High volatility in budget balance = unpredictable fiscal management.
Rising debt trajectory = deteriorating fiscal sustainability.

Score construction:
  1. Balance volatility component: std(budget_balance) / 3 * 50, capped at 50.
     (annual std > 3 ppts of GDP implies poor fiscal discipline)
  2. Debt trajectory component:
     - Latest debt level: stress if > 60% (Maastricht threshold).
       debt_level_penalty = clip((debt - 60) / 40 * 30, 0, 30)
     - Debt trend (slope over history):
       If slope > 0 (rising), debt_trend_penalty = min(20, slope * 5)
  3. score = min(100, vol_component + debt_level_penalty + debt_trend_penalty)

Fallback: if only one indicator available, use it alone with adjusted formula.

Sources: World Bank WDI (GC.BAL.CASH.GD.ZS, GC.DOD.TOTL.GD.ZS)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class FiscalGovernance(LayerBase):
    layer_id = "lGV"
    name = "Fiscal Governance"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('GC.BAL.CASH.GD.ZS', 'GC.DOD.TOTL.GD.ZS')
            ORDER BY ds.series_id, dp.date
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        series: dict[str, list[float]] = {}
        series_dates: dict[str, list[str]] = {}
        for r in rows:
            sid = r["series_id"]
            series.setdefault(sid, []).append(float(r["value"]))
            series_dates.setdefault(sid, []).append(r["date"])

        if not series:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        # Budget balance volatility
        vol_component = 0.0
        balance_std = None
        balance_latest = None
        if "GC.BAL.CASH.GD.ZS" in series:
            bal = np.array(series["GC.BAL.CASH.GD.ZS"])
            balance_latest = float(bal[-1])
            if len(bal) >= 3:
                balance_std = float(np.std(bal, ddof=1))
                vol_component = min(50.0, balance_std / 3.0 * 50.0)
            else:
                # Single obs: penalty based on deficit size
                vol_component = min(30.0, max(0.0, -balance_latest * 3.0))

        # Debt trajectory
        debt_level_penalty = 0.0
        debt_trend_penalty = 0.0
        debt_latest = None
        debt_slope = None
        if "GC.DOD.TOTL.GD.ZS" in series:
            debt = np.array(series["GC.DOD.TOTL.GD.ZS"])
            debt_latest = float(debt[-1])
            debt_level_penalty = float(np.clip((debt_latest - 60.0) / 40.0 * 30.0, 0.0, 30.0))
            if len(debt) >= 3:
                x = np.arange(len(debt), dtype=float)
                result = linregress(x, debt)
                debt_slope = float(result.slope)
                if debt_slope > 0:
                    debt_trend_penalty = min(20.0, debt_slope * 5.0)

        score = float(np.clip(vol_component + debt_level_penalty + debt_trend_penalty, 0.0, 100.0))

        all_dates = [d for dates in series_dates.values() for d in dates]

        return {
            "score": round(score, 1),
            "country": country,
            "budget_balance_latest_pct_gdp": round(balance_latest, 2)
            if balance_latest is not None
            else None,
            "budget_balance_std": round(balance_std, 4) if balance_std is not None else None,
            "debt_pct_gdp_latest": round(debt_latest, 2) if debt_latest is not None else None,
            "debt_slope_per_year": round(debt_slope, 4) if debt_slope is not None else None,
            "components": {
                "balance_volatility": round(vol_component, 2),
                "debt_level_penalty": round(debt_level_penalty, 2),
                "debt_trend_penalty": round(debt_trend_penalty, 2),
            },
            "indicators_used": list(series.keys()),
            "period": f"{min(all_dates)} to {max(all_dates)}",
        }
