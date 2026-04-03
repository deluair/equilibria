"""Active labor market programme (ALMP) spending as % of GDP.

ALMPs include job-search assistance, training, subsidized employment, and
entrepreneurship support — policies designed to improve matching and re-employment
prospects. The OECD average is around 0.5% of GDP; spending above 1% of GDP
reflects highly interventionist regimes (Denmark, Sweden).

Very low spending (below 0.1% GDP) leaves displaced workers without institutional
support, amplifying cyclical unemployment persistence.

Scoring (very low spending -> high stress; diminishing returns above 1.5%):
    score = clip(100 - almp_pct_gdp * 67, 0, 100)

    almp = 1.5%  -> score = 0   (generous, no stress)
    almp = 0.5%  -> score = 67  (OECD average; moderate)
    almp = 0.1%  -> score = 93  (very low)
    almp = 0.0%  -> score = 100 (no ALMP)

Sources: OECD SOCX (almp — ALMP expenditure as % GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

SERIES = "almp_gdp_pct"


class ActiveLaborMarketSpending(LayerBase):
    layer_id = "lLI"
    name = "Active Labor Market Spending"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'almp_gdp_pct'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no ALMP spending data (almp_gdp_pct)",
            }

        latest_date = rows[0]["date"]
        almp_pct = float(rows[0]["value"])

        score = float(np.clip(100.0 - almp_pct * 66.67, 0.0, 100.0))

        if almp_pct >= 1.0:
            effort = "high"
        elif almp_pct >= 0.4:
            effort = "moderate"
        elif almp_pct >= 0.1:
            effort = "low"
        else:
            effort = "negligible"

        trend_direction = "insufficient data"
        recent = sorted(rows[:10], key=lambda r: r["date"])
        if len(recent) >= 3:
            vals = np.array([float(r["value"]) for r in recent], dtype=float)
            slope = float(np.polyfit(np.arange(len(vals), dtype=float), vals, 1)[0])
            trend_direction = "rising" if slope > 0.02 else "falling" if slope < -0.02 else "stable"

        return {
            "score": round(score, 2),
            "country": country,
            "almp_pct_gdp": round(almp_pct, 3),
            "almp_effort": effort,
            "trend": trend_direction,
            "latest_date": latest_date,
            "n_obs": len(rows),
            "note": (
                "score = clip(100 - almp_pct * 66.67, 0, 100). "
                "OECD avg ~ 0.5%. Series: almp_gdp_pct."
            ),
        }
