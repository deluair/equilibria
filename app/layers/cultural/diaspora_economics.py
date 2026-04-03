"""Diaspora Economics module.

Remittance dependency vs FDI: high remittances with low FDI signals
diaspora dependency rather than integrated capital markets.

Queries:
- BX.TRF.PWKR.DT.GD.ZS : Personal remittances received (% of GDP)
- BX.KLT.DINV.WD.GD.ZS  : FDI net inflows (% of GDP)

Stress score is based on the remittance-to-FDI ratio. A high ratio
(heavy remittance reliance, weak FDI) indicates diaspora dependency.
Benchmarks: remittance/FDI ratio > 3 -> elevated stress.

score = clip((ratio - 1) / 5 * 100, 0, 100)
where ratio = remittance_pct / max(fdi_pct, 0.1)

Sources: WDI (BX.TRF.PWKR.DT.GD.ZS, BX.KLT.DINV.WD.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

REMITTANCE_SERIES = "BX.TRF.PWKR.DT.GD.ZS"
FDI_SERIES = "BX.KLT.DINV.WD.GD.ZS"


class DiasporaEconomics(LayerBase):
    layer_id = "lCU"
    name = "Diaspora Economics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('BX.TRF.PWKR.DT.GD.ZS', 'BX.KLT.DINV.WD.GD.ZS')
            ORDER BY ds.series_id, dp.date DESC
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data (need >= 5 rows)",
            }

        latest: dict[str, float] = {}
        for r in rows:
            sid = r["series_id"]
            if sid not in latest:
                latest[sid] = float(r["value"])

        remittance_pct = latest.get(REMITTANCE_SERIES)
        fdi_pct = latest.get(FDI_SERIES)

        if remittance_pct is None or fdi_pct is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "missing remittance or FDI series",
            }

        # Avoid division by near-zero or negative FDI
        fdi_denom = max(float(fdi_pct), 0.1)
        ratio = float(remittance_pct) / fdi_denom

        # score rises with ratio; ratio=1 -> ~0, ratio=6 -> ~100
        score = float(np.clip((ratio - 1.0) / 5.0 * 100.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(rows),
            "remittance_pct_gdp": round(float(remittance_pct), 4),
            "fdi_pct_gdp": round(float(fdi_pct), 4),
            "remittance_fdi_ratio": round(ratio, 4),
            "note": "ratio = remittance_pct / max(fdi_pct, 0.1); score = clip((ratio-1)/5*100, 0, 100)",
        }
