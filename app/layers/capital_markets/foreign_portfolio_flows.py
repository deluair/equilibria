"""Foreign portfolio investment flow volatility.

Portfolio investment inflows (BX.PEF.TOTL.CD.WD) are inherently volatile,
prone to sudden reversals ('sudden stops') that destabilise domestic capital
markets. High inflow volatility increases systemic fragility.

Score (0-100): high coefficient of variation of flows = high stress.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ForeignPortfolioFlows(LayerBase):
    layer_id = "lCK"
    name = "Foreign Portfolio Flows"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 15)

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.indicator_code = 'BX.PEF.TOTL.CD.WD'
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
                "error": "insufficient portfolio flow data (need >= 3 observations)",
            }

        values = np.array([float(r["value"]) for r in rows])
        flow_mean = float(np.mean(values))
        flow_std = float(np.std(values, ddof=1))
        flow_latest = float(values[-1])

        # Coefficient of variation (normalise by absolute mean to handle sign)
        abs_mean = abs(flow_mean) if abs(flow_mean) > 1e-6 else 1.0
        cv = flow_std / abs_mean

        # Sudden-stop indicator: last observation is negative vs positive mean
        sudden_stop = flow_latest < 0 and flow_mean > 0

        score = float(np.clip(cv * 30.0, 0.0, 100.0))
        if sudden_stop:
            score = float(np.clip(score + 20.0, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "portfolio_flows_usd": {
                "latest": round(flow_latest, 0),
                "mean": round(flow_mean, 0),
                "std_dev": round(flow_std, 0),
                "coefficient_of_variation": round(cv, 3),
                "observations": len(values),
            },
            "sudden_stop_flag": sudden_stop,
            "volatility_level": (
                "high" if cv > 2.0
                else "moderate" if cv > 0.8
                else "low"
            ),
        }
