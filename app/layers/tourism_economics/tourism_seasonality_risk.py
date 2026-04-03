"""Tourism Seasonality Risk module.

Measures intra-series volatility in international tourist arrivals
(ST.INT.ARVL) via the coefficient of variation (CoV). High CoV indicates
strong seasonality or irregular arrivals, creating economic instability for
tourism-dependent businesses and workers.

Score: 0 (smooth, year-round demand) to 100 (extreme seasonal concentration).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TourismSeasonalityRisk(LayerBase):
    layer_id = "lTO"
    name = "Tourism Seasonality Risk"

    async def compute(self, db, **kwargs) -> dict:
        code = "ST.INT.ARVL"
        name = "international arrivals"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for ST.INT.ARVL (international arrivals) for seasonality",
            }

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if len(values) < 3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient observations for seasonality CoV (need >= 3)",
            }

        arr = np.array(values)
        mean_val = float(np.mean(arr))
        std_val = float(np.std(arr, ddof=1))
        cov = std_val / mean_val if mean_val > 0 else 0.0

        # CoV 0 -> score 10, CoV 0.5 -> score 60, CoV 1+ -> score 95
        score = float(np.clip(cov * 85 + 10, 0, 100))

        return {
            "score": round(score, 1),
            "indicator": code,
            "mean_arrivals": round(mean_val, 0),
            "std_arrivals": round(std_val, 0),
            "cov": round(cov, 3),
            "n_obs": len(values),
            "high_seasonality_risk": cov > 0.4,
            "methodology": "score = clip(cov * 85 + 10, 0, 100); CoV of annual arrivals series",
        }
