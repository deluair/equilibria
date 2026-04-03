"""Mortgage Market Depth module.

Mortgage credit as a share of GDP proxy. Uses domestic credit to private
sector (FS.AST.PRVT.GD.ZS). Very low depth (<15% GDP) signals financial
exclusion from housing; very high depth (>130% GDP) signals systemic
over-leverage. Deviation from a 60% benchmark captures both extremes.

Score = clip(abs(credit_gdp - 60) * 0.9, 0, 100)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MortgageMarketDepth(LayerBase):
    layer_id = "lHO"
    name = "Mortgage Market Depth"

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
                "error": "insufficient private credit data for mortgage market depth",
            }

        vals = np.array([float(r["value"]) for r in rows])
        latest = float(vals[-1])
        trend = float(np.mean(np.diff(vals[-5:]))) if len(vals) >= 6 else float(np.mean(np.diff(vals)))

        raw_score = abs(latest - 60) * 0.9
        score = float(np.clip(raw_score, 0, 100))

        depth_class = "SHALLOW" if latest < 15 else ("DEEP" if latest > 130 else "MODERATE")

        return {
            "score": round(score, 1),
            "country": country,
            "mortgage_credit_gdp_pct": round(latest, 2),
            "benchmark_pct": 60.0,
            "deviation_from_benchmark": round(latest - 60, 2),
            "trend_annual_change": round(trend, 2),
            "depth_classification": depth_class,
            "n_obs": len(rows),
            "methodology": "score = clip(abs(credit_gdp - 60) * 0.9, 0, 100)",
        }
