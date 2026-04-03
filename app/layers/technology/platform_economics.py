"""Platform Economics module.

Platform economy readiness: logistics + digital + financial inclusion.

Low internet penetration + low bank account ownership = platform economy underdevelopment.

Sources: WDI (IT.NET.USER.ZS, FX.OWN.TOTL.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_INTERNET_BENCHMARK = 60.0   # % internet penetration for platform readiness
_FININCL_BENCHMARK = 70.0    # % with bank account for financial inclusion readiness


class PlatformEconomics(LayerBase):
    layer_id = "lTE"
    name = "Platform Economics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        internet_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'IT.NET.USER.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )
        finincl_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'FX.OWN.TOTL.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not internet_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no internet penetration data",
            }

        internet_vals = np.array([float(r["value"]) for r in internet_rows])
        internet_pct = float(internet_vals[-1])

        # Digital gap component (0-60 pts)
        digital_gap = max(0.0, _INTERNET_BENCHMARK - internet_pct)
        digital_score = float(np.clip(digital_gap / _INTERNET_BENCHMARK * 60.0, 0.0, 60.0))

        # Financial inclusion gap component (0-40 pts)
        finincl_score = 0.0
        finincl_pct = None
        if finincl_rows:
            finincl_vals = np.array([float(r["value"]) for r in finincl_rows])
            finincl_pct = float(finincl_vals[-1])
            finincl_gap = max(0.0, _FININCL_BENCHMARK - finincl_pct)
            finincl_score = float(np.clip(finincl_gap / _FININCL_BENCHMARK * 40.0, 0.0, 40.0))
        else:
            # No financial inclusion data: apply moderate penalty
            finincl_score = 20.0

        score = float(np.clip(digital_score + finincl_score, 0.0, 100.0))

        result = {
            "score": round(score, 1),
            "country": country,
            "internet_pct_latest": round(internet_pct, 2),
            "digital_benchmark_pct": _INTERNET_BENCHMARK,
            "digital_gap_pct": round(digital_gap, 2),
            "digital_score_component": round(digital_score, 1),
            "finincl_score_component": round(finincl_score, 1),
            "internet_n_obs": len(internet_rows),
            "interpretation": (
                "low digital adoption + low financial inclusion = platform economy underdevelopment"
            ),
        }
        if finincl_pct is not None:
            result["bank_account_ownership_pct_latest"] = round(finincl_pct, 2)
            result["finincl_benchmark_pct"] = _FININCL_BENCHMARK
            result["finincl_n_obs"] = len(finincl_rows)

        return result
