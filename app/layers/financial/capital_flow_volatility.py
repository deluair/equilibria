"""Capital flow volatility analysis.

FDI inflows as a share of GDP (BX.KLT.DINV.WD.GD.ZS) exhibit year-to-year
swings that signal sudden-stop risk. Standard deviation of the time series
is the primary stress metric.

Score (0-100): clip(fdi_stddev * 5, 0, 100).
High volatility pushes toward CRISIS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CapitalFlowVolatility(LayerBase):
    layer_id = "l7"
    name = "Capital Flow Volatility"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 15)

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.indicator_code = 'BX.KLT.DINV.WD.GD.ZS'
              AND ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
            ORDER BY dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not rows or len(rows) < 3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient FDI data (need >= 3 observations)",
            }

        values = np.array([float(r["value"]) for r in rows])
        fdi_mean = float(np.mean(values))
        fdi_stddev = float(np.std(values, ddof=1))
        fdi_latest = float(values[-1])

        score = float(np.clip(fdi_stddev * 5.0, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "fdi_inflows_pct_gdp": {
                "latest": round(fdi_latest, 3),
                "mean": round(fdi_mean, 3),
                "std_dev": round(fdi_stddev, 3),
                "observations": len(values),
            },
            "sudden_stop_risk": (
                "high" if fdi_stddev > 10
                else "moderate" if fdi_stddev > 5
                else "low"
            ),
            "indicator": "BX.KLT.DINV.WD.GD.ZS",
        }
