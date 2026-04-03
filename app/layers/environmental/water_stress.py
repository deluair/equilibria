"""Water stress: freshwater withdrawal relative to internal resources.

Queries World Bank WDI series ER.H2O.FWTL.ZS (annual freshwater withdrawals
as % of internal resources). High withdrawal relative to availability signals
water scarcity and supply security risk.

Score = clip(withdrawal_pct * 0.8, 0, 100):
  - withdrawal_pct < 10%  -> low stress (score < 8)
  - withdrawal_pct = 40%  -> moderate stress (score ~32)
  - withdrawal_pct >= 100% -> severe overextraction (score capped at 100)

Sources: World Bank WDI (ER.H2O.FWTL.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class WaterStress(LayerBase):
    layer_id = "l9"
    name = "Water Stress"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3")

        if not country:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "country_iso3 required",
            }

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'ER.H2O.FWTL.ZS'
              AND ds.country_iso3 = ?
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no freshwater withdrawal data",
            }

        # Use most recent non-null value
        withdrawal_pct = None
        latest_year = None
        for r in rows:
            if r["value"] is not None:
                withdrawal_pct = float(r["value"])
                latest_year = r["date"][:4]
                break

        if withdrawal_pct is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "all withdrawal values are null",
            }

        score = float(np.clip(withdrawal_pct * 0.8, 0, 100))

        stress_level = (
            "severe" if withdrawal_pct >= 80
            else "high" if withdrawal_pct >= 40
            else "moderate" if withdrawal_pct >= 20
            else "low"
        )

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": country,
                "series_id": "ER.H2O.FWTL.ZS",
                "latest_year": latest_year,
                "withdrawal_pct_internal_resources": round(withdrawal_pct, 2),
                "stress_level": stress_level,
                "overextracted": withdrawal_pct >= 100,
            },
        }
