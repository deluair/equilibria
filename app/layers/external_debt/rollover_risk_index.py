"""Rollover Risk Index module.

Measures the ratio of near-maturing external debt to foreign exchange reserves.
A high ratio means the country may lack reserves to roll over or repay maturing
obligations, signalling acute liquidity risk.

Methodology:
- Query DT.DOD.DSTC.CD (short-term external debt, current USD) as maturing proxy.
- Query FI.RES.TOTL.CD (total reserves including gold, current USD).
- Rollover risk = short_term_debt / reserves.
- Score = clip(rollover_risk / 2.0 * 100, 0, 100): ratio >= 2 = max stress.

Sources: World Bank WDI (DT.DOD.DSTC.CD, FI.RES.TOTL.CD)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RolloverRiskIndex(LayerBase):
    layer_id = "lXD"
    name = "Rollover Risk Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        st_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'DT.DOD.DSTC.CD'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        res_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FI.RES.TOTL.CD'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not st_rows or not res_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no rollover risk data"}

        st_map = {r["date"]: float(r["value"]) for r in st_rows if r["value"] is not None}
        res_map = {r["date"]: float(r["value"]) for r in res_rows if r["value"] is not None}

        common = sorted(set(st_map) & set(res_map), reverse=True)
        if not common:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no overlapping dates"}

        ref_date = common[0]
        st_debt = st_map[ref_date]
        reserves = res_map[ref_date]

        if reserves <= 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "reserves zero or negative"}

        rollover_risk = st_debt / reserves
        score = float(np.clip(rollover_risk / 2.0 * 100, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "rollover_risk_ratio": round(rollover_risk, 4),
            "short_term_debt_usd": st_debt,
            "reserves_usd": reserves,
            "reference_date": ref_date,
            "acute_risk": rollover_risk > 1.0,
            "indicators": ["DT.DOD.DSTC.CD", "FI.RES.TOTL.CD"],
        }
