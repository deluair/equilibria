"""Cultural Consumption module.

Education expenditure as a proxy for cultural investment.

Queries SE.XPD.TOTL.GD.ZS (Government expenditure on education, % of GDP).
The UNESCO benchmark is 4-6% of GDP. Below 4% indicates under-investment
in human and cultural capital; above 6% is treated as fully adequate.

Scoring formula:
  score = clip((4.0 - spend_pct) / 4.0 * 100, 0, 100)
  - spend >= 4% -> score = 0 or negative -> clipped to 0 (no stress)
  - spend = 2%  -> score = 50
  - spend = 0%  -> score = 100

Uses the most recent available value; time-series average used as
robustness check when only aggregate data is available.

Sources: WDI (SE.XPD.TOTL.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

EDU_SERIES = "SE.XPD.TOTL.GD.ZS"
BENCHMARK_PCT = 4.0


class CulturalConsumption(LayerBase):
    layer_id = "lCU"
    name = "Cultural Consumption"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SE.XPD.TOTL.GD.ZS'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data (need >= 5 rows)",
            }

        values = np.array([float(r["value"]) for r in rows], dtype=float)
        latest_val = float(values[0])
        mean_val = float(np.mean(values))

        # Primary: most recent; secondary signal from mean
        score = float(np.clip((BENCHMARK_PCT - latest_val) / BENCHMARK_PCT * 100.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(rows),
            "edu_spend_latest_pct_gdp": round(latest_val, 4),
            "edu_spend_mean_pct_gdp": round(mean_val, 4),
            "benchmark_pct": BENCHMARK_PCT,
            "period": f"{rows[-1]['date']} to {rows[0]['date']}",
            "note": "score = clip((4.0 - spend_pct) / 4.0 * 100, 0, 100); UNESCO benchmark = 4% GDP",
        }
