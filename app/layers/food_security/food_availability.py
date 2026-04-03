"""Cereal production per capita trend as a food availability stress indicator.

Combines the FAO crop production index (WDI: AG.PRD.CROP.XD) with population
(WDI: SP.POP.TOTL) to derive per-capita crop output, then applies OLS linear
regression to measure the trend direction. A declining slope signals that the
country is producing less food per person over time.

Methodology:
    per_capita_index_t = crop_production_index_t / population_t * 1e6

    Regress: per_capita_index_t = alpha + beta * t + e_t

    Score = clip(-beta_normalized * 100, 0, 100)

    where beta_normalized = slope / mean(per_capita_index) captures
    the proportional annual change.

Benchmarks:
    beta_normalized = 0: stable, no stress (score = 0).
    beta_normalized = -0.02 (2% annual decline): moderate stress (score ~67).
    beta_normalized <= -0.03: score -> 100 (crisis).

Score (0-100): Higher score = greater availability stress.

References:
    FAO (2022). "FAOSTAT Production Domain."
    World Bank (2023). World Development Indicators: AG.PRD.CROP.XD, SP.POP.TOTL.
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class FoodAvailability(LayerBase):
    layer_id = "lFS"
    name = "Food Availability"

    async def compute(self, db, **kwargs) -> dict:
        """Compute cereal production per capita trend and stress score.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            min_obs : int - minimum observations required (default 5)
        """
        country = kwargs.get("country_iso3", "BGD")
        min_obs = kwargs.get("min_obs", 5)

        crop_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.indicator_code = 'AG.PRD.CROP.XD'
            ORDER BY dp.date ASC
            """,
            (country,),
        )
        if not crop_rows or len(crop_rows) < min_obs:
            crop_rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE '%crop%production%index%'
                ORDER BY dp.date ASC
                """,
                (country,),
            )

        pop_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.indicator_code = 'SP.POP.TOTL'
            ORDER BY dp.date ASC
            """,
            (country,),
        )
        if not pop_rows or len(pop_rows) < min_obs:
            pop_rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE '%population%total%'
                ORDER BY dp.date ASC
                """,
                (country,),
            )

        if not crop_rows or len(crop_rows) < min_obs:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient crop production data",
            }

        crop_map: dict[int, float] = {}
        for r in crop_rows:
            if r["value"] is not None:
                try:
                    crop_map[int(str(r["date"])[:4])] = float(r["value"])
                except (ValueError, TypeError):
                    continue

        pop_map: dict[int, float] = {}
        for r in pop_rows:
            if r["value"] is not None:
                try:
                    pop_map[int(str(r["date"])[:4])] = float(r["value"])
                except (ValueError, TypeError):
                    continue

        years = sorted(crop_map.keys())
        per_capita: list[tuple[int, float]] = []
        for yr in years:
            if yr in pop_map and pop_map[yr] > 0:
                per_capita.append((yr, crop_map[yr] / pop_map[yr] * 1e6))
            elif crop_map.get(yr) is not None:
                per_capita.append((yr, crop_map[yr]))

        if len(per_capita) < min_obs:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"insufficient matched observations (need >= {min_obs})",
            }

        t = np.array([p[0] for p in per_capita], dtype=float)
        y = np.array([p[1] for p in per_capita], dtype=float)

        result = linregress(t, y)
        slope = float(result.slope)
        r_squared = float(result.rvalue ** 2)
        p_value = float(result.pvalue)

        mean_y = float(np.mean(y))
        beta_normalized = slope / mean_y if mean_y > 0 else 0.0

        score = float(np.clip(-beta_normalized / 0.03 * 100, 0.0, 100.0))

        trend_direction = (
            "declining" if slope < 0 and p_value < 0.10
            else "stagnant" if abs(beta_normalized) < 0.005
            else "growing"
        )

        return {
            "score": round(score, 2),
            "country": country,
            "trend": {
                "slope_per_year": round(slope, 6),
                "beta_normalized": round(beta_normalized, 6),
                "r_squared": round(r_squared, 4),
                "p_value": round(p_value, 4),
                "direction": trend_direction,
            },
            "latest_per_capita_index": round(float(y[-1]), 4),
            "mean_per_capita_index": round(mean_y, 4),
            "n_obs": len(y),
            "period": {"start": int(t[0]), "end": int(t[-1])},
            "pop_data_available": len(pop_map) > 0,
            "indicators": ["AG.PRD.CROP.XD", "SP.POP.TOTL"],
        }
