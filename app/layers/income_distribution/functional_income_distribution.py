"""Functional Income Distribution module.

Measures labor vs capital share of income by comparing labor productivity
trajectory to GDP per capita. A growing divergence between productivity
and income per worker signals rising capital share and shrinking labor share.

Score derived from the trend in productivity-income divergence: persistent
and widening gap -> higher stress score (capital appropriating gains).

Sources: WDI (SL.GDP.PCAP.EM.KD, NY.GDP.PCAP.KD)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FunctionalIncomeDistribution(LayerBase):
    layer_id = "lID"
    name = "Functional Income Distribution"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        labor_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.GDP.PCAP.EM.KD'
            ORDER BY dp.date
            """,
            (country,),
        )

        gdp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not labor_rows or not gdp_rows or len(labor_rows) < 5 or len(gdp_rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        # Align by date
        labor_map = {r["date"]: float(r["value"]) for r in labor_rows}
        gdp_map = {r["date"]: float(r["value"]) for r in gdp_rows}
        common_dates = sorted(set(labor_map) & set(gdp_map))

        if len(common_dates) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient overlapping dates"}

        labor_vals = np.array([labor_map[d] for d in common_dates])
        gdp_vals = np.array([gdp_map[d] for d in common_dates])

        # Normalize to index (first period = 100) to compare trajectories
        labor_idx = labor_vals / labor_vals[0] * 100
        gdp_idx = gdp_vals / gdp_vals[0] * 100

        # Divergence: how much productivity outpaces income per capita
        divergence = labor_idx - gdp_idx
        n = len(divergence)

        # Trend in divergence: positive slope = capital share rising over time
        t = np.arange(n)
        slope = float(np.polyfit(t, divergence, 1)[0]) if n >= 3 else 0.0
        current_gap = float(divergence[-1])
        mean_gap = float(np.mean(divergence))

        # Score: positive gap (productivity > income) = capital capturing more
        # slope > 0 means gap is widening
        gap_score = np.clip(max(0.0, current_gap) * 0.5, 0, 50)
        trend_score = np.clip(max(0.0, slope) * 10, 0, 50)
        score = float(np.clip(gap_score + trend_score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": n,
            "period": f"{common_dates[0]} to {common_dates[-1]}",
            "current_divergence_index_pts": round(current_gap, 2),
            "mean_divergence_index_pts": round(mean_gap, 2),
            "divergence_trend_per_year": round(slope, 4),
            "interpretation": (
                "positive divergence = labor productivity outpacing income per capita, "
                "consistent with rising capital share"
            ),
        }
