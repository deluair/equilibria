"""External Debt Composition module.

Measures the mix of concessional vs commercial (non-concessional) external debt.
Higher concessional share implies lower debt-service burden and longer maturities,
reducing stress. A rising commercial share signals crowding into harder terms.

Methodology:
- Query DT.DOD.PCBK.CD (commercial bank and other creditors debt, current USD).
- Query DT.DOD.DECT.CD (total external debt stocks, current USD).
- Commercial share = commercial / total.
- Score = clip(commercial_share * 100, 0, 100): fully commercial = max stress.

Sources: World Bank WDI (DT.DOD.PCBK.CD, DT.DOD.DECT.CD)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ExternalDebtComposition(LayerBase):
    layer_id = "lXD"
    name = "External Debt Composition"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        commercial_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'DT.DOD.PCBK.CD'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        total_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'DT.DOD.DECT.CD'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not commercial_rows or not total_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no debt composition data"}

        commercial_map = {r["date"]: float(r["value"]) for r in commercial_rows if r["value"] is not None}
        total_map = {r["date"]: float(r["value"]) for r in total_rows if r["value"] is not None}

        common_dates = sorted(set(commercial_map) & set(total_map), reverse=True)
        if not common_dates:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no overlapping dates"}

        ref_date = common_dates[0]
        commercial = commercial_map[ref_date]
        total = total_map[ref_date]

        if total <= 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "total debt zero or negative"}

        commercial_share = commercial / total
        score = float(np.clip(commercial_share * 100, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "commercial_share": round(commercial_share, 4),
            "concessional_share": round(1.0 - commercial_share, 4),
            "reference_date": ref_date,
            "commercial_debt_usd": commercial,
            "total_debt_usd": total,
            "indicators": ["DT.DOD.PCBK.CD", "DT.DOD.DECT.CD"],
        }
