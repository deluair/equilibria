"""Expenditure Composition module.

Measures the quality of public spending: productive and social expenditure
vs. interest payments. High interest costs crowding out social and capital
spending is a marker of fiscal deterioration.

Methodology:
- Query GC.XPN.TRFT.ZS (social transfers, % expenditure) for social share.
- Query NE.GDI.FTOT.ZS (gross fixed capital formation, % GDP) as capital proxy.
- Query GC.XPN.INTP.CN / GC.XPN.TOTL.CN for interest share of expenditure.
- High interest + low social = crowding out.
- Score = clip(interest_share * 5 - social_share * 0.5, 0, 100).

Sources: World Bank WDI (GC.XPN.TRFT.ZS, NE.GDI.FTOT.ZS,
         GC.XPN.INTP.CN, GC.XPN.TOTL.CN)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ExpenditureComposition(LayerBase):
    layer_id = "lFP"
    name = "Expenditure Composition"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        async def _latest(series_id: str) -> float | None:
            rows = await db.fetch_all(
                """
                SELECT dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country, series_id),
            )
            if rows:
                return float(rows[0]["value"])
            return None

        social_share = await _latest("GC.XPN.TRFT.ZS")
        gfcf = await _latest("NE.GDI.FTOT.ZS")
        intp = await _latest("GC.XPN.INTP.CN")
        totl = await _latest("GC.XPN.TOTL.CN")

        # Interest share of expenditure (%)
        interest_share: float | None = None
        if intp is not None and totl is not None and abs(totl) > 1e-10:
            interest_share = (intp / totl) * 100

        if interest_share is None and social_share is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        s = social_share if social_share is not None else 0.0
        i = interest_share if interest_share is not None else 0.0

        score = float(np.clip(i * 5 - s * 0.5, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "social_transfers_pct_expenditure": round(s, 3),
            "gfcf_pct_gdp": round(gfcf, 3) if gfcf is not None else None,
            "interest_pct_expenditure": round(i, 3),
            "crowding_out": i > 15,
            "indicators": [
                "GC.XPN.TRFT.ZS",
                "NE.GDI.FTOT.ZS",
                "GC.XPN.INTP.CN",
                "GC.XPN.TOTL.CN",
            ],
        }
