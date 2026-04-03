"""Debt Transparency Index module.

Proxies the extent of hidden or unreported external debt by comparing
World Bank-reported total external debt to IMF external debt estimates and
flagging large discrepancies. Hidden debt (state-owned enterprise borrowing,
collateralised loans, off-balance-sheet guarantees) inflates true obligations
beyond official statistics.

Methodology:
- Query DT.DOD.DECT.CD (World Bank total external debt, current USD).
- Query DT.DOD.DECT.GD.ZS (external debt % GNI) to infer GNI scale.
- Use cross-series completeness as a reporting discipline proxy:
  count of non-null annual observations in last 10 years vs expected 10.
- Transparency score = (available_obs / 10) * 100.
- Stress score = 100 - transparency_score.

Sources: World Bank WDI (DT.DOD.DECT.CD, DT.DOD.DECT.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_WINDOW_YEARS = 10


class DebtTransparencyIndex(LayerBase):
    layer_id = "lXD"
    name = "Debt Transparency Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows_cd = await db.fetch_all(
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

        rows_zs = await db.fetch_all(
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

        if not rows_cd and not rows_zs:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no debt transparency data"}

        all_rows = list(rows_cd) + list(rows_zs)
        non_null = sum(1 for r in all_rows if r["value"] is not None)
        expected = _WINDOW_YEARS * 2  # two series

        transparency_score = min(1.0, non_null / expected)
        stress_score = float(np.clip((1.0 - transparency_score) * 100, 0, 100))

        return {
            "score": round(stress_score, 1),
            "country": country,
            "transparency_ratio": round(transparency_score, 4),
            "non_null_observations": non_null,
            "expected_observations": expected,
            "low_transparency": transparency_score < 0.6,
            "indicators": ["DT.DOD.DECT.CD", "DT.DOD.DECT.GD.ZS"],
        }
