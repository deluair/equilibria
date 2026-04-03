"""Demographic Pension Pressure module.

Tracks the youth-to-elderly ratio trend to quantify accelerating pension
pressure. A declining ratio of working-age youth relative to the elderly
signals that future contributor pools are shrinking while beneficiary pools
grow. Uses linregress on the ratio trend over time.

Score = clip(max(0, -slope_normalized) * 100 + base_pressure, 0, 100)

Sources: WDI SP.POP.0014.TO.ZS (youth % of total population),
         WDI SP.POP.65UP.TO.ZS (elderly % of total population)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class DemographicPensionPressure(LayerBase):
    layer_id = "lPS"
    name = "Demographic Pension Pressure"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        youth_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.POP.0014.TO.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        elderly_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.POP.65UP.TO.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not youth_rows or not elderly_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no youth or elderly population data",
            }

        youth_map = {
            r["date"]: float(r["value"]) for r in youth_rows if r["value"] is not None
        }
        elderly_map = {
            r["date"]: float(r["value"]) for r in elderly_rows if r["value"] is not None
        }

        common_dates = sorted(set(youth_map) & set(elderly_map))
        if len(common_dates) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient overlapping data"}

        ratios = np.array(
            [youth_map[d] / max(elderly_map[d], 0.01) for d in common_dates]
        )
        x = np.arange(len(ratios), dtype=float)
        slope, intercept, r_val, p_val, _ = linregress(x, ratios)

        current_ratio = float(ratios[-1])
        # Normalize slope relative to current ratio level
        slope_normalized = float(slope) / max(current_ratio, 0.01)

        # Base pressure from current ratio (low ratio = fewer workers per retiree)
        base_pressure = float(np.clip(max(0.0, (5.0 - current_ratio) * 10.0), 0, 50))
        # Trend pressure from declining ratio
        trend_pressure = float(np.clip(max(0.0, -slope_normalized) * 100.0, 0, 50))

        score = float(np.clip(base_pressure + trend_pressure, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "current_youth_elderly_ratio": round(current_ratio, 3),
            "slope_per_year": round(float(slope), 4),
            "slope_normalized": round(slope_normalized, 6),
            "r_squared": round(float(r_val**2), 4),
            "p_value": round(float(p_val), 4),
            "ratio_declining": slope < 0,
            "n_obs": len(ratios),
            "period": f"{common_dates[0]} to {common_dates[-1]}",
            "base_pressure": round(base_pressure, 1),
            "trend_pressure": round(trend_pressure, 1),
            "interpretation": (
                "accelerating pension pressure" if score > 75
                else "significant demographic pressure" if score > 50
                else "moderate demographic pressure" if score > 25
                else "demographic pressure manageable"
            ),
            "sources": ["WDI SP.POP.0014.TO.ZS", "WDI SP.POP.65UP.TO.ZS"],
        }
