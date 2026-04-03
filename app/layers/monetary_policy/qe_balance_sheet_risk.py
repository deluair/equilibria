"""QE Balance Sheet Risk: central bank balance sheet / GDP and exit risk.

Methodology
-----------
Unconventional monetary policy (QE) expands the central bank balance sheet.
Risks arise from:
  1. Asset price sensitivity: large portfolio subject to mark-to-market losses
  2. Exit complexity: unwinding large portfolios risks market disruption
  3. Fiscal dominance: very large balance sheet signals QE-financed deficits

Bernanke (2015), Borio & Disyatat (2010):
  Balance sheet / GDP > 20% historically associated with exit risk
  Balance sheet / GDP > 50% (Japan-level) = CRISIS

Score = clip((bs_gdp_ratio / 50) * 100, 0, 100)
  5%  -> score 10 (STABLE, pre-GFC norm)
  20% -> score 40 (WATCH, post-GFC developed markets)
  50% -> score 100 (CRISIS, Japan 2023 level)

Sources: WDI FM.AST.DOMS.CN (domestic credit by banking sector proxy),
         IMF HBSE (central bank balance sheet % GDP where available)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class QeBalanceSheetRisk(LayerBase):
    layer_id = "lMY"
    name = "QE Balance Sheet Risk"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        lookback = kwargs.get("lookback_years", 15)

        bs_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'HBSE'
              AND dp.date >= date('now', ?)
            ORDER BY dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not bs_rows:
            # Fallback: domestic credit as proxy
            bs_rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = 'FM.AST.DOMS.GD.ZS'
                  AND dp.date >= date('now', ?)
                ORDER BY dp.date
                """,
                (country, f"-{lookback} years"),
            )

        if not bs_rows or len(bs_rows) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no balance sheet data"}

        bs_vals = np.array([float(r["value"]) for r in bs_rows])
        dates = [r["date"] for r in bs_rows]
        bs_latest = float(bs_vals[-1])
        bs_change = float(bs_vals[-1] - bs_vals[0])
        bs_trend = float(np.polyfit(np.arange(len(bs_vals)), bs_vals, 1)[0])

        score = float(np.clip((bs_latest / 50.0) * 100.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "balance_sheet_gdp_pct_latest": round(bs_latest, 2),
            "balance_sheet_gdp_pct_start": round(float(bs_vals[0]), 2),
            "change_over_period_pp": round(bs_change, 2),
            "trend_per_year_pp": round(bs_trend, 3),
            "expanding": bs_trend > 0,
            "exit_risk_level": (
                "low" if bs_latest < 20
                else "moderate" if bs_latest < 40
                else "high"
            ),
            "n_obs": len(bs_rows),
            "period": f"{dates[0]} to {dates[-1]}",
        }
