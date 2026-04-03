"""Wage Growth Gap module.

Measures divergence between labor productivity growth and wage/income growth.
When productivity outpaces wages, workers are not capturing the gains from
growth -- a signal of wage suppression.

Score = clip(max(0, prod_growth - wage_growth) * 5, 0, 100).

Sources: WDI (SL.GDP.PCAP.EM.KD.ZG for labor productivity growth,
NY.GDP.PCAP.KD.ZG as wage growth proxy)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class WageGrowthGap(LayerBase):
    layer_id = "lID"
    name = "Wage Growth Gap"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        prod_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.GDP.PCAP.EM.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        wage_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not prod_rows or not wage_rows or len(prod_rows) < 5 or len(wage_rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        prod_map = {r["date"]: float(r["value"]) for r in prod_rows}
        wage_map = {r["date"]: float(r["value"]) for r in wage_rows}
        common = sorted(set(prod_map) & set(wage_map))

        if len(common) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient overlapping dates"}

        prod_vals = np.array([prod_map[d] for d in common])
        wage_vals = np.array([wage_map[d] for d in common])

        gap = prod_vals - wage_vals  # positive = productivity outpacing wages

        mean_prod = float(np.mean(prod_vals))
        mean_wage = float(np.mean(wage_vals))
        mean_gap = float(np.mean(gap))
        recent_gap = float(np.mean(gap[-3:])) if len(gap) >= 3 else mean_gap

        # Trend: is the gap widening?
        t = np.arange(len(gap))
        slope = float(np.polyfit(t, gap, 1)[0]) if len(gap) >= 3 else 0.0

        score = float(np.clip(max(0.0, recent_gap) * 5, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(common),
            "period": f"{common[0]} to {common[-1]}",
            "mean_productivity_growth_pct": round(mean_prod, 3),
            "mean_income_growth_pct": round(mean_wage, 3),
            "mean_gap_pct": round(mean_gap, 3),
            "recent_gap_pct": round(recent_gap, 3),
            "gap_trend_per_year": round(slope, 4),
            "interpretation": "positive gap = productivity outpacing income growth (wage suppression signal)",
        }
