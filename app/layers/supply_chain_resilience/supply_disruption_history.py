"""Supply disruption history: CoV of import volume growth.

Uses NE.IMP.GNFS.KD.ZG (imports of goods and services, annual % growth). The
coefficient of variation (CoV) of historical import growth captures realized
supply disruption frequency and magnitude.

Methodology:
    Fetch up to 15 observations of NE.IMP.GNFS.KD.ZG. Compute mean and standard
    deviation of growth rates. CoV = std / abs(mean).
    score = clip(CoV * 50, 0, 100).

    CoV = 0: score = 0 (perfectly stable import growth, no disruptions).
    CoV = 2: score = 100 (extreme volatility, frequent disruptions).
    CoV = 1 (typical emerging market): score = 50.

    Edge case: if mean is near 0, use std directly scaled.

Score (0-100): Higher score indicates more supply disruption history.

References:
    World Bank WDI NE.IMP.GNFS.KD.ZG.
    Hendricks & Singhal (2005). "An empirical analysis of the effect of supply chain
    disruptions on long-run stock price performance." Production & Operations Mgmt.
"""

from __future__ import annotations

import math
import statistics

from app.layers.base import LayerBase

_CODE = "NE.IMP.GNFS.KD.ZG"
_NAME = "imports of goods and services"


class SupplyDisruptionHistory(LayerBase):
    layer_id = "lSR"
    name = "Supply Disruption History"

    async def compute(self, db, **kwargs) -> dict:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_CODE, f"%{_NAME}%"),
        )

        values = [float(r["value"]) for r in rows if r["value"] is not None]

        if len(values) < 3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"insufficient data for CoV computation ({_CODE}), need >= 3 obs",
            }

        mean_growth = statistics.mean(values)
        std_growth = statistics.stdev(values)
        abs_mean = abs(mean_growth)

        if abs_mean > 0.5:
            cov = std_growth / abs_mean
        else:
            cov = std_growth / 10.0

        score = float(min(max(cov * 50.0, 0.0), 100.0))

        neg_count = sum(1 for v in values if v < 0)

        return {
            "score": round(score, 2),
            "mean_import_growth_pct": round(mean_growth, 2),
            "std_import_growth_pct": round(std_growth, 2),
            "coefficient_of_variation": round(cov, 4),
            "negative_growth_episodes": neg_count,
            "n_obs": len(values),
            "indicator": _CODE,
        }
