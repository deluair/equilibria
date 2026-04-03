"""Debt Service Ratio module.

Measures total external debt service (principal + interest) as a share of
export earnings. A high ratio crowds out import capacity and signals liquidity
stress; values above 20-25% are conventionally considered distress thresholds.

Methodology:
- Query DT.TDS.DECT.EX.ZS (total debt service, % of exports).
- Latest available value determines the score.
- Score = clip(value / 0.4, 0, 100): 40% service/exports = max stress.

Sources: World Bank WDI (DT.TDS.DECT.EX.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DebtServiceRatio(LayerBase):
    layer_id = "lXD"
    name = "Debt Service Ratio"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'DT.TDS.DECT.EX.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no debt service data"}

        latest = next((r for r in rows if r["value"] is not None), None)
        if latest is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all values null"}

        ratio = float(latest["value"])
        score = float(np.clip(ratio / 0.4, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "debt_service_pct_exports": round(ratio, 2),
            "reference_date": latest["date"],
            "distress_threshold_exceeded": ratio > 20.0,
            "indicators": ["DT.TDS.DECT.EX.ZS"],
        }
