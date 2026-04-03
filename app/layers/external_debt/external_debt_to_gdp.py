"""External Debt to GDP module.

Measures the ratio of total external debt stock to GDP. High ratios signal
vulnerability to external shocks, currency crises, and rollover risk.

Methodology:
- Query DT.DOD.DECT.GD.ZS (external debt stocks, % of GNI) as proxy for external debt/GDP.
- Latest available value determines the score.
- Score = clip(value / 1.5, 0, 100) so 150% debt/GDP = max stress.

Sources: World Bank WDI (DT.DOD.DECT.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ExternalDebtToGdp(LayerBase):
    layer_id = "lXD"
    name = "External Debt to GDP"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'DT.DOD.DECT.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no external debt/GNI data"}

        latest = next((r for r in rows if r["value"] is not None), None)
        if latest is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all values null"}

        ratio = float(latest["value"])
        score = float(np.clip(ratio / 1.5, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "external_debt_pct_gni": round(ratio, 2),
            "reference_date": latest["date"],
            "high_debt": ratio > 60.0,
            "indicators": ["DT.DOD.DECT.GD.ZS"],
        }
