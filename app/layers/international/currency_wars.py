"""Currency Wars module.

Measures real exchange rate volatility as a proxy for competitive devaluation
pressure (Eichengreen 2013; Bergsten & Gagnon 2017). High coefficient of variation
in the official exchange rate signals currency instability or active exchange-rate
manipulation. Uses official exchange rate (LCU per USD) as primary series; falls
back to PX.REX (real effective exchange rate index) if available.

Score = clip(CV * 200, 0, 100), where CV = std / mean of exchange rate values.

Sources: WDI (PA.NUS.FCRF official rate LCU/USD; PX.REX.TOTL real effective exchange rate)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CurrencyWars(LayerBase):
    layer_id = "lIN"
    name = "Currency Wars"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        # Try REER first for a real measure of competitiveness
        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('PX.REX.TOTL', 'PA.NUS.FCRF')
            ORDER BY dp.date ASC
            LIMIT 30
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no exchange rate data found",
            }

        valid_values = [float(r["value"]) for r in rows if r["value"] is not None]

        if len(valid_values) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient exchange rate observations (need >= 5)",
            }

        arr = np.array(valid_values, dtype=float)
        mean_val = float(np.mean(arr))
        std_val = float(np.std(arr, ddof=1))

        if mean_val <= 1e-10:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "exchange rate mean near zero, cannot compute CV",
            }

        cv = std_val / abs(mean_val)

        # Period-on-period changes (year-to-year log returns)
        log_changes = np.diff(np.log(np.abs(arr) + 1e-15))
        volatility_annual = float(np.std(log_changes, ddof=1)) if len(log_changes) > 1 else 0.0

        score = float(np.clip(cv * 200, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(valid_values),
            "coefficient_of_variation": round(cv, 6),
            "exchange_rate_mean": round(mean_val, 4),
            "exchange_rate_std": round(std_val, 4),
            "log_return_volatility": round(volatility_annual, 6),
            "high_volatility": cv > 0.3,
        }
