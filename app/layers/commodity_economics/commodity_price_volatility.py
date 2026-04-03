"""Commodity Price Volatility module.

Measures the volatility of commodity prices using the coefficient of variation
(CoV) of a commodity price index. High price volatility raises macroeconomic
uncertainty, destabilizes export revenues, and complicates fiscal planning.

Methodology:
- Query a commodity price index series (e.g. PALL_INDEX or PBCOM_USD).
- Compute CoV = std(prices) / mean(prices) over the available window.
- Normalize to 0-100 score: score = clip(CoV * 200, 0, 100).
  CoV of 0.50 (50%) maps to score 100 (extreme volatility).

Sources: World Bank Pink Sheet (PBCOM_USD), IMF WEO commodity indices.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CommodityPriceVolatility(LayerBase):
    layer_id = "lCM"
    name = "Commodity Price Volatility"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "WLD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'PBCOM_USD'
            ORDER BY dp.date DESC
            LIMIT 60
            """,
            (),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no commodity price data"}

        values = np.array([float(row["value"]) for row in rows])
        mean_price = float(np.mean(values))
        std_price = float(np.std(values, ddof=1)) if len(values) > 1 else 0.0
        cov = std_price / mean_price if mean_price > 0 else 0.0

        score = float(np.clip(cov * 200, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "coefficient_of_variation": round(cov, 4),
            "mean_price": round(mean_price, 2),
            "std_price": round(std_price, 2),
            "n_obs": len(values),
            "high_volatility": cov > 0.25,
            "indicators": ["PBCOM_USD"],
        }
