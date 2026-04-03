"""Financial deepening index.

Combines private credit (FS.AST.DOMS.GD.ZS) and stock market capitalisation
(CM.MKT.LCAP.GD.ZS) as a share of GDP. This composite captures both bank-
based and market-based financial intermediation depth, following the
King & Levine (1993) and Rajan & Zingales (1998) frameworks.

Score (0-100): low combined ratio = shallow financial system = stress.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FinancialDeepeningIndex(LayerBase):
    layer_id = "lCK"
    name = "Financial Deepening Index"

    # Benchmark: combined ratio > 150% = deep financial system
    DEEP_THRESHOLD = 150.0

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 10)

        rows = await db.fetch_all(
            """
            SELECT ds.indicator_code, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.indicator_code IN ('FS.AST.DOMS.GD.ZS', 'CM.MKT.LCAP.GD.ZS')
              AND ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
            ORDER BY ds.indicator_code, dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no financial deepening data",
            }

        by_code: dict[str, list[float]] = {}
        for r in rows:
            by_code.setdefault(r["indicator_code"], []).append(float(r["value"]))

        credit_vals = by_code.get("FS.AST.DOMS.GD.ZS", [])
        mktcap_vals = by_code.get("CM.MKT.LCAP.GD.ZS", [])

        if not credit_vals and not mktcap_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "neither credit nor market cap series available",
            }

        credit_latest = float(credit_vals[-1]) if credit_vals else 0.0
        mktcap_latest = float(mktcap_vals[-1]) if mktcap_vals else 0.0

        combined_ratio = credit_latest + mktcap_latest

        # Track over time if both available
        combined_trend = None
        if credit_vals and mktcap_vals and len(credit_vals) >= 2 and len(mktcap_vals) >= 2:
            min_len = min(len(credit_vals), len(mktcap_vals))
            combined_series = [
                credit_vals[i] + mktcap_vals[i] for i in range(min_len)
            ]
            combined_trend = combined_series[-1] - combined_series[0]

        score = float(np.clip(100.0 * (1.0 - combined_ratio / self.DEEP_THRESHOLD), 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "financial_deepening": {
                "private_credit_pct_gdp": round(credit_latest, 2) if credit_vals else None,
                "market_cap_pct_gdp": round(mktcap_latest, 2) if mktcap_vals else None,
                "combined_pct_gdp": round(combined_ratio, 2),
                "trend_pp": round(combined_trend, 2) if combined_trend is not None else None,
                "benchmark_pct_gdp": self.DEEP_THRESHOLD,
            },
            "depth_category": (
                "very_shallow" if combined_ratio < 30
                else "shallow" if combined_ratio < 70
                else "moderate" if combined_ratio < self.DEEP_THRESHOLD
                else "deep"
            ),
        }
