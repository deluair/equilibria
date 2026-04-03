"""Countercyclical Capital Buffer (CCyB) signal.

Basel III CCyB framework: credit-to-GDP gap above HP-filtered trend.
A gap exceeding 2 percentage points signals buffer activation.

Score (0-100): clip(max(0, gap) * 10, 0, 100).
Gap > 10pp maps to score 100 (CRISIS).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CountercyclicalBuffer(LayerBase):
    layer_id = "lMP"
    name = "Countercyclical Capital Buffer"

    # Hodrick-Prescott smoothing parameter for annual data (BIS recommendation)
    HP_LAMBDA = 400_000

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 20)

        rows = await db.fetch_all(
            """
            SELECT ds.series_code, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_code = 'FS.AST.DOMS.GD.ZS'
              AND ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
            ORDER BY dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if len(rows) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient credit-to-GDP data for HP filter",
            }

        values = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]

        trend = self._hp_filter(values, self.HP_LAMBDA)
        gap = values - trend
        latest_gap = float(gap[-1])
        latest_actual = float(values[-1])
        latest_trend = float(trend[-1])

        score = float(np.clip(max(0.0, latest_gap) * 10.0, 0.0, 100.0))

        buffer_signal = latest_gap > 2.0

        return {
            "score": round(score, 2),
            "country": country,
            "credit_to_gdp_pct": round(latest_actual, 2),
            "hp_trend_pct": round(latest_trend, 2),
            "credit_gap_pp": round(latest_gap, 4),
            "buffer_activation_signal": buffer_signal,
            "gap_threshold_pp": 2.0,
            "hp_lambda": self.HP_LAMBDA,
            "observations": len(values),
            "date_range": {"start": dates[0], "end": dates[-1]},
            "interpretation": self._interpret(latest_gap, buffer_signal),
        }

    @staticmethod
    def _hp_filter(y: np.ndarray, lam: float) -> np.ndarray:
        """Hodrick-Prescott filter via matrix algebra (annual lambda=400000)."""
        n = len(y)
        # Second-difference matrix
        D = np.zeros((n - 2, n))
        for i in range(n - 2):
            D[i, i] = 1
            D[i, i + 1] = -2
            D[i, i + 2] = 1
        I = np.eye(n)
        A = I + lam * D.T @ D
        return np.linalg.solve(A, y)

    @staticmethod
    def _interpret(gap: float, signal: bool) -> str:
        if signal:
            return f"CCyB activation warranted: credit gap {gap:.1f}pp above trend"
        if gap > 0:
            return f"Credit above trend by {gap:.1f}pp: monitoring phase"
        return f"Credit below trend by {abs(gap):.1f}pp: no buffer pressure"
