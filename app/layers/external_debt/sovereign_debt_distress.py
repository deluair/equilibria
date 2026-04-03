"""Sovereign Debt Distress module.

Estimates DSA-style debt distress probability by combining the external
debt-to-GNI ratio with the debt service-to-exports ratio into a composite
stress index, consistent with IMF/World Bank Low-Income Country DSF thresholds.

Methodology:
- Query DT.DOD.DECT.GD.ZS (external debt, % GNI).
- Query DT.TDS.DECT.EX.ZS (debt service, % exports).
- DSF indicative thresholds (moderate capacity): debt/GNI 55%, service/exports 15%.
- Component scores = value / threshold * 50 each.
- Total score = clip(debt_score + service_score, 0, 100).

Sources: World Bank WDI; IMF DSF thresholds (2018 revised framework)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# IMF DSF thresholds for medium debt-carrying capacity
_DEBT_GNI_THRESHOLD = 55.0   # percent
_SERVICE_EX_THRESHOLD = 15.0  # percent


class SovereignDebtDistress(LayerBase):
    layer_id = "lXD"
    name = "Sovereign Debt Distress"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        debt_rows = await db.fetch_all(
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

        service_rows = await db.fetch_all(
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

        if not debt_rows or not service_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient DSA data"}

        debt_map = {r["date"]: float(r["value"]) for r in debt_rows if r["value"] is not None}
        service_map = {r["date"]: float(r["value"]) for r in service_rows if r["value"] is not None}

        common = sorted(set(debt_map) & set(service_map), reverse=True)
        if not common:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no overlapping dates"}

        ref_date = common[0]
        debt_pct = debt_map[ref_date]
        service_pct = service_map[ref_date]

        debt_score = min(50.0, debt_pct / _DEBT_GNI_THRESHOLD * 50.0)
        service_score = min(50.0, service_pct / _SERVICE_EX_THRESHOLD * 50.0)
        score = float(np.clip(debt_score + service_score, 0, 100))

        distress_rating = "LOW"
        if score >= 75:
            distress_rating = "HIGH"
        elif score >= 50:
            distress_rating = "MODERATE"

        return {
            "score": round(score, 1),
            "country": country,
            "distress_rating": distress_rating,
            "debt_pct_gni": round(debt_pct, 2),
            "debt_service_pct_exports": round(service_pct, 2),
            "dsf_debt_threshold": _DEBT_GNI_THRESHOLD,
            "dsf_service_threshold": _SERVICE_EX_THRESHOLD,
            "reference_date": ref_date,
            "indicators": ["DT.DOD.DECT.GD.ZS", "DT.TDS.DECT.EX.ZS"],
        }
