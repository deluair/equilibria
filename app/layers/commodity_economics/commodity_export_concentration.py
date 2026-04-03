"""Commodity Export Concentration module.

Measures the Herfindahl-Hirschman Index (HHI) of the commodity export
portfolio. High concentration in a few commodities increases vulnerability
to price shocks in those commodities.

Methodology:
- Query export value shares for major commodity groups (fuels, ores & metals,
  agricultural raw materials, food) from WDI.
- Compute HHI = sum(s_i^2) where s_i is share of commodity group i in total
  merchandise exports.
- Normalize: HHI ranges from near 0 (diversified) to 1 (fully concentrated).
- Score = clip(HHI * 100, 0, 100).

Sources: World Bank WDI (TX.VAL.FUEL.ZS.UN, TX.VAL.MMTL.ZS.UN,
         TX.VAL.AGRI.ZS.UN, TX.VAL.FOOD.ZS.UN).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_COMMODITY_SERIES = [
    ("fuels", "TX.VAL.FUEL.ZS.UN"),
    ("ores_metals", "TX.VAL.MMTL.ZS.UN"),
    ("agri_raw", "TX.VAL.AGRI.ZS.UN"),
    ("food", "TX.VAL.FOOD.ZS.UN"),
]


class CommodityExportConcentration(LayerBase):
    layer_id = "lCM"
    name = "Commodity Export Concentration"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        shares: dict[str, float] = {}
        for label, series_id in _COMMODITY_SERIES:
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
                shares[label] = float(rows[0]["value"])

        if len(shares) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient export composition data"}

        share_values = np.array(list(shares.values())) / 100.0
        # Normalize to sum to 1 across available groups
        total = share_values.sum()
        if total > 0:
            share_values = share_values / total

        hhi = float(np.sum(share_values ** 2))
        score = float(np.clip(hhi * 100, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "hhi": round(hhi, 4),
            "shares": {k: round(shares[k], 2) for k in shares},
            "high_concentration": hhi > 0.4,
            "indicators": [s for _, s in _COMMODITY_SERIES],
        }
