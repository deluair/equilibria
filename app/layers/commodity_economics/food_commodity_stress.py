"""Food Commodity Stress module.

Constructs a food commodity price stress index based on cereal, oil crop,
and food aggregate price indices relative to their historical benchmarks.

Methodology:
- Query FAO Food Price Index (PFOOD_USD) or World Bank food price series.
- Query cereal price index (PWHEAMT_USD or PMAIZ_USD as proxies).
- Compute z-scores of current price vs. 5-year rolling mean.
- Stress index = mean z-score, clamped and normalized to 0-100.
  z-score > 2 (2 std above mean) = high stress.
- score = clip((mean_z + 2) / 4 * 100, 0, 100).

Sources: World Bank Pink Sheet (PFOOD_USD, PWHEAMT_USD, PMAIZ_USD).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_FOOD_SERIES = ["PFOOD_USD", "PWHEAMT_USD", "PMAIZ_USD"]


class FoodCommodityStress(LayerBase):
    layer_id = "lCM"
    name = "Food Commodity Stress"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "WLD")

        z_scores: list[float] = []
        latest_prices: dict[str, float] = {}

        for series_id in _FOOD_SERIES:
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT 60
                """,
                (series_id,),
            )
            if len(rows) < 6:
                continue
            vals = [float(r["value"]) for r in reversed(rows)]
            latest = vals[-1]
            reference = vals[:-1]
            mean_ref = float(np.mean(reference))
            std_ref = float(np.std(reference, ddof=1)) if len(reference) > 1 else 1.0
            z = (latest - mean_ref) / max(std_ref, 1e-6)
            z_scores.append(z)
            latest_prices[series_id] = round(latest, 2)

        if not z_scores:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no food price data"}

        mean_z = float(np.mean(z_scores))
        score = float(np.clip((mean_z + 2) / 4 * 100, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "mean_z_score": round(mean_z, 3),
            "individual_z_scores": {s: round(z, 3) for s, z in zip(_FOOD_SERIES, z_scores)},
            "latest_prices": latest_prices,
            "high_stress": mean_z > 1.5,
            "indicators": _FOOD_SERIES,
        }
