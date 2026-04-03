"""Land Value Taxation module.

Measures land rent efficiency via overall tax capacity as a proxy for
how well land value is being captured by the fiscal system.

Queries GC.TAX.TOTL.GD.ZS (tax revenue % of GDP). Low tax capacity
signals land value is not being adequately captured, leaving economic
rents unrealized for public benefit.

Score = clip(max(0, 25 - tax_gdp_ratio) * 2, 0, 100)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class LandValueTaxation(LayerBase):
    layer_id = "lRE"
    name = "Land Value Taxation"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.TAX.TOTL.GD.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient tax revenue data for land value taxation analysis",
            }

        vals = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]

        latest = float(vals[-1])
        avg = float(np.mean(vals[-5:])) if len(vals) >= 5 else float(np.mean(vals))
        trend = float(np.mean(np.diff(vals[-4:]))) if len(vals) >= 5 else float(np.mean(np.diff(vals))) if len(vals) > 1 else 0.0

        # Low tax/GDP ratio = land value not captured = stress
        raw_score = max(0.0, 25.0 - latest) * 2.0
        score = float(np.clip(raw_score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "tax_gdp_pct_latest": round(latest, 2),
            "tax_gdp_pct_avg": round(avg, 2),
            "tax_gdp_trend": round(trend, 3),
            "benchmark_pct": 25.0,
            "period": f"{dates[0]} to {dates[-1]}",
            "n_obs": len(rows),
            "methodology": "score = clip(max(0, 25 - tax_gdp_ratio) * 2, 0, 100); low tax capacity = land rent unrealized",
        }
