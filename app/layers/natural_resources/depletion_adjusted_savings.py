"""Depletion-adjusted savings: genuine savings net of resource depletion.

Genuine (adjusted net) savings = gross savings - fixed capital consumption
  - energy depletion - mineral depletion - net forest depletion
  + education expenditure + pollution damage adjustment.

Uses World Bank WDI:
  NY.ADJ.SVNG.GN.ZS  - adjusted net savings including particulate emission damage (% GNI)

Negative genuine savings signals unsustainable resource draw-down.

Score = clip(max(0, 50 - savings_pct * 2), 0, 100):
  - savings_pct >= 25%  -> very sustainable (score ~0)
  - savings_pct = 0%    -> breakeven (score 50)
  - savings_pct = -25%  -> severe depletion (score 100)

Sources: World Bank WDI (NY.ADJ.SVNG.GN.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DepletionAdjustedSavings(LayerBase):
    layer_id = "lNR"
    name = "Depletion Adjusted Savings"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.ADJ.SVNG.GN.ZS'
              AND ds.country_iso3 = ?
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no genuine savings data",
            }

        valid = [(r["date"][:4], float(r["value"])) for r in rows if r["value"] is not None]
        if not valid:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "all genuine savings values are null",
            }

        latest_year, savings_pct = valid[0]
        # Trend: average of up to 5 recent years
        recent_vals = [v for _, v in valid[:5]]
        trend_avg = float(np.mean(recent_vals))

        score = float(np.clip(50.0 - savings_pct * 2.0, 0, 100))

        sustainability = (
            "unsustainable" if savings_pct < 0
            else "marginal" if savings_pct < 5
            else "adequate" if savings_pct < 15
            else "strong"
        )

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": country,
                "series_id": "NY.ADJ.SVNG.GN.ZS",
                "latest_year": latest_year,
                "genuine_savings_pct_gni": round(savings_pct, 3),
                "5yr_avg_pct_gni": round(trend_avg, 3),
                "sustainability": sustainability,
                "n_years": len(valid),
            },
        }
