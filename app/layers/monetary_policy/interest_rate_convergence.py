"""Interest Rate Convergence: domestic vs global rate convergence/divergence.

Methodology
-----------
Uncovered Interest Parity (UIP) and covered interest parity imply that
in integrated capital markets, domestic rates converge to global rates
adjusted for risk premium and expected depreciation.

Persistent large spreads between domestic and global (US/EU) rates indicate:
  - Risk premium elevation (sovereign/credit risk)
  - Capital controls or market segmentation
  - Monetary policy divergence from global stance

Obstfeld & Taylor (2004), Bluedorn & Bowdler (2011):
  spread = domestic_rate - global_rate
  |spread| < 3pp -> convergence (integrated)
  |spread| > 10pp -> divergence (segmented/crisis)

Score = clip(|spread| * 5, 0, 100)
  spread = 0  -> score 0 (STABLE, fully integrated)
  spread = 10 -> score 50 (WATCH)
  spread = 20 -> score 100 (CRISIS)

Sources: IMF FIDR (domestic), FEDFUNDS/ECB rate (global benchmark)
         WDI FR.INR.LEND as fallback for domestic lending rate
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_GLOBAL_BENCHMARKS = ["FEDFUNDS", "ECB_RATE", "GLOBAL_RATE"]


class InterestRateConvergence(LayerBase):
    layer_id = "lMY"
    name = "Interest Rate Convergence"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        lookback = kwargs.get("lookback_years", 10)

        domestic_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FIDR'
              AND dp.date >= date('now', ?)
            ORDER BY dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not domestic_rows:
            domestic_rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = 'FR.INR.LEND'
                  AND dp.date >= date('now', ?)
                ORDER BY dp.date
                """,
                (country, f"-{lookback} years"),
            )

        if not domestic_rows or len(domestic_rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no domestic rate data"}

        # Try to fetch a global benchmark
        global_rows: list[dict] = []
        global_series_used = ""
        for bench in _GLOBAL_BENCHMARKS:
            global_rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.series_id = ?
                  AND dp.date >= date('now', ?)
                ORDER BY dp.date
                """,
                (bench, f"-{lookback} years"),
            )
            if global_rows:
                global_series_used = bench
                break

        domestic_map = {r["date"]: float(r["value"]) for r in domestic_rows}
        domestic_latest = float(list(domestic_map.values())[-1])
        domestic_dates = sorted(domestic_map)

        spread: float | None = None
        spread_history: list[float] = []

        if global_rows:
            global_map = {r["date"]: float(r["value"]) for r in global_rows}
            common = sorted(set(domestic_map) & set(global_map))
            if common:
                spread_history = [domestic_map[d] - global_map[d] for d in common]
                spread = float(spread_history[-1])
                global_latest = float(global_map[common[-1]])
            else:
                global_latest = None
        else:
            global_latest = None
            # Without global data, use within-country rate trend as proxy
            domestic_arr = np.array([domestic_map[d] for d in domestic_dates])
            trend = float(np.polyfit(np.arange(len(domestic_arr)), domestic_arr, 1)[0])
            # Cannot compute convergence without peer; return limited result
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no global benchmark rate data available",
                "country": country,
                "domestic_rate_latest": round(domestic_latest, 2),
                "domestic_rate_trend_per_year": round(trend, 3),
            }

        abs_spread = abs(spread)
        spread_vol = float(np.std(spread_history, ddof=1)) if len(spread_history) > 1 else 0.0
        score = float(np.clip(abs_spread * 5.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "domestic_rate_latest": round(domestic_latest, 2),
            "global_benchmark_rate": round(global_latest, 2) if global_latest is not None else None,
            "spread_pp": round(spread, 2),
            "abs_spread_pp": round(abs_spread, 2),
            "spread_volatility_pp": round(spread_vol, 3),
            "convergence_status": (
                "converged" if abs_spread < 3
                else "diverging" if abs_spread < 10
                else "highly_divergent"
            ),
            "global_benchmark_used": global_series_used,
            "n_obs": len(domestic_rows),
            "period": f"{domestic_dates[0]} to {domestic_dates[-1]}",
        }
