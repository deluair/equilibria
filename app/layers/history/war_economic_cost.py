"""War Economic Cost module.

Uses military expenditure as a share of GDP as a proxy for the economic
burden of conflict and militarisation. Sustained high military spending
crowds out productive investment and signals an ongoing conflict economy.

Indicator: MS.MIL.XPND.GD.ZS (Military expenditure, % of GDP, WDI/SIPRI).
Thresholds: > 3% = elevated; > 5% = crisis.
Score: clip(latest_value * 15, 0, 100).
  - 0%  -> 0   (no burden)
  - 3%  -> 45  (STRESS threshold)
  - 5%  -> 75  (near-CRISIS)
  - 6.7%-> 100 (full crisis)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class WarEconomicCost(LayerBase):
    layer_id = "lHI"
    name = "War Economic Cost"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'MS.MIL.XPND.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        latest_value = float(rows[0]["value"])
        values = [float(r["value"]) for r in rows]
        avg_5yr = float(np.mean(values[:5])) if len(values) >= 5 else float(np.mean(values))

        score = float(np.clip(latest_value * 15, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "latest_mil_pct_gdp": round(latest_value, 3),
            "avg_5yr_mil_pct_gdp": round(avg_5yr, 3),
            "latest_year": rows[0]["date"][:4],
            "n_obs": len(rows),
            "elevated_threshold_pct": 3.0,
            "crisis_threshold_pct": 5.0,
        }
