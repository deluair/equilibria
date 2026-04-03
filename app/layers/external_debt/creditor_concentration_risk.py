"""Creditor Concentration Risk module.

Measures the Herfindahl-Hirschman Index (HHI) of creditor composition in
external debt. High concentration (few dominant creditors) increases
negotiation leverage of creditors, reduces rescheduling flexibility, and
amplifies geopolitical risk in debt management.

Methodology:
- Use three creditor categories from World Bank WDI:
    DT.DOD.BLAT.CD: bilateral (official, government-to-government)
    DT.DOD.MLAT.CD: multilateral (IFIs)
    DT.DOD.PCBK.CD: commercial banks and other private
- HHI = sum of squared shares = sum((creditor_i / total)^2).
- HHI in [0,1]: 1/3 = equal split (minimum with 3), 1 = monopoly.
- Score = clip((HHI - 1/3) / (1 - 1/3) * 100, 0, 100).

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_SERIES = {
    "bilateral": "DT.DOD.BLAT.CD",
    "multilateral": "DT.DOD.MLAT.CD",
    "commercial": "DT.DOD.PCBK.CD",
}
_MIN_HHI = 1 / 3  # equal 3-way split


class CreditorConcentrationRisk(LayerBase):
    layer_id = "lXD"
    name = "Creditor Concentration Risk"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        category_maps: dict[str, dict[str, float]] = {}
        for label, series_id in _SERIES.items():
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT 10
                """,
                (country, series_id),
            )
            category_maps[label] = {r["date"]: float(r["value"]) for r in rows if r["value"] is not None}

        # Find a reference date present in all three series
        common = sorted(
            set(category_maps["bilateral"]) & set(category_maps["multilateral"]) & set(category_maps["commercial"]),
            reverse=True,
        )

        if not common:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no overlapping creditor data"}

        ref_date = common[0]
        values = {label: category_maps[label][ref_date] for label in _SERIES}
        total = sum(values.values())

        if total <= 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "total creditor debt zero"}

        shares = {label: v / total for label, v in values.items()}
        hhi = sum(s ** 2 for s in shares.values())
        score = float(np.clip((hhi - _MIN_HHI) / (1.0 - _MIN_HHI) * 100, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "hhi": round(hhi, 4),
            "bilateral_share": round(shares["bilateral"], 4),
            "multilateral_share": round(shares["multilateral"], 4),
            "commercial_share": round(shares["commercial"], 4),
            "reference_date": ref_date,
            "high_concentration": hhi > 0.5,
            "indicators": list(_SERIES.values()),
        }
