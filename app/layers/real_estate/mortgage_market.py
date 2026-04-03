"""Mortgage Market Depth module.

Proxies mortgage market depth via domestic credit to private sector
(FS.AST.PRVT.GD.ZS). Very low credit (<15% GDP) indicates underdevelopment;
very high (>150% GDP) signals overleveraging. Score captures stress at
both extremes relative to a 75% benchmark.

Score = clip(abs(value - 75) * 0.8, 0, 100)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MortgageMarket(LayerBase):
    layer_id = "lRE"
    name = "Mortgage Market"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FS.AST.PRVT.GD.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient private credit data for mortgage market assessment",
            }

        vals = np.array([float(r["value"]) for r in rows])
        latest = float(vals[-1])
        trend = float(np.mean(np.diff(vals[-5:]))) if len(vals) >= 6 else float(np.mean(np.diff(vals)))

        # Stress at both extremes: underdeveloped (<15) or overleveraged (>150)
        # Benchmark: 75% of GDP as moderate depth
        raw_score = abs(latest - 75) * 0.8
        score = float(np.clip(raw_score, 0, 100))

        stress_type = "UNDERDEVELOPED" if latest < 15 else ("OVERLEVERAGED" if latest > 150 else "MODERATE")

        return {
            "score": round(score, 1),
            "country": country,
            "private_credit_gdp_pct": round(latest, 2),
            "benchmark_pct": 75.0,
            "deviation_from_benchmark": round(latest - 75, 2),
            "trend_annual_change": round(trend, 2),
            "stress_type": stress_type,
            "n_obs": len(rows),
            "methodology": "score = clip(abs(private_credit_gdp - 75) * 0.8, 0, 100)",
        }
