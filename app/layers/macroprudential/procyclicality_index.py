"""Procyclicality Index.

Credit procyclicality: Pearson correlation between annual domestic credit
growth (pp change in credit-to-GDP) and real GDP growth. Strong positive
correlation indicates that credit amplifies business cycles.

Score (0-100): clip(max(0, corr) * 100, 0, 100).
Perfect positive correlation = score 100 (CRISIS).
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class ProcyclicalityIndex(LayerBase):
    layer_id = "lMP"
    name = "Procyclicality Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 20)

        rows = await db.fetch_all(
            """
            SELECT ds.series_code, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_code IN ('FS.AST.DOMS.GD.ZS', 'NY.GDP.MKTP.KD.ZG')
              AND ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
            ORDER BY ds.series_code, dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no credit or GDP growth data",
            }

        credit_by_date: dict[str, float] = {}
        gdp_growth_by_date: dict[str, float] = {}

        for r in rows:
            if r["series_code"] == "FS.AST.DOMS.GD.ZS":
                credit_by_date[r["date"]] = float(r["value"])
            elif r["series_code"] == "NY.GDP.MKTP.KD.ZG":
                gdp_growth_by_date[r["date"]] = float(r["value"])

        # Align by date and compute credit growth (annual pp change)
        credit_dates_sorted = sorted(credit_by_date.keys())
        credit_growth_by_date: dict[str, float] = {}
        for i in range(1, len(credit_dates_sorted)):
            d_curr = credit_dates_sorted[i]
            d_prev = credit_dates_sorted[i - 1]
            credit_growth_by_date[d_curr] = credit_by_date[d_curr] - credit_by_date[d_prev]

        common_dates = sorted(set(credit_growth_by_date) & set(gdp_growth_by_date))

        if len(common_dates) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"insufficient overlapping observations: {len(common_dates)} (need 5)",
            }

        credit_growth_arr = np.array([credit_growth_by_date[d] for d in common_dates])
        gdp_growth_arr = np.array([gdp_growth_by_date[d] for d in common_dates])

        corr, p_value = sp_stats.pearsonr(credit_growth_arr, gdp_growth_arr)
        corr = float(corr)

        score = float(np.clip(max(0.0, corr) * 100.0, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "pearson_correlation": round(corr, 4),
            "p_value": round(float(p_value), 4),
            "statistically_significant": p_value < 0.10,
            "common_observations": len(common_dates),
            "date_range": {"start": common_dates[0], "end": common_dates[-1]},
            "procyclicality_direction": "procyclical" if corr > 0.3 else "countercyclical" if corr < -0.3 else "neutral",
            "interpretation": self._interpret(corr, p_value),
        }

    @staticmethod
    def _interpret(corr: float, p: float) -> str:
        sig = "significant" if p < 0.10 else "not statistically significant"
        if corr > 0.6:
            return f"strong procyclicality (r={corr:.2f}, {sig}): credit amplifies cycles"
        if corr > 0.3:
            return f"moderate procyclicality (r={corr:.2f}, {sig})"
        if corr < -0.3:
            return f"countercyclical credit behavior (r={corr:.2f}, {sig})"
        return f"weak cyclicality (r={corr:.2f}, {sig})"
