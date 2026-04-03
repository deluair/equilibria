"""Open Innovation module.

Proxies openness of the innovation system through international linkages:
- FDI net inflows as % of GDP (BX.KLT.DINV.WD.GD.ZS)
- High-technology exports as % of manufactured exports (TX.VAL.TECH.MF.ZS)

Low FDI combined with low technology exports signals a closed innovation system
that lacks external knowledge flows and global technology network participation.
Both channels bring foreign knowledge, embodied technology, and R&D spillovers.

Score: higher = more closed innovation system (higher stress).

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class OpenInnovation(LayerBase):
    layer_id = "lNV"
    name = "Open Innovation"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        fdi_rows = await db.fetch_all(
            """
            SELECT dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'BX.KLT.DINV.WD.GD.ZS'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        hitech_rows = await db.fetch_all(
            """
            SELECT dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'TX.VAL.TECH.MF.ZS'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        fdi: float | None = None
        hitech: float | None = None

        if fdi_rows:
            vals = [float(r["value"]) for r in fdi_rows if r["value"] is not None]
            fdi = float(np.mean(vals)) if vals else None

        if hitech_rows:
            vals = [float(r["value"]) for r in hitech_rows if r["value"] is not None]
            hitech = float(np.mean(vals)) if vals else None

        if fdi is None and hitech is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        normed: list[float] = []
        if fdi is not None:
            # FDI inflows: 0-20% GDP -> 0-100 (clamp negative FDI at 0)
            normed.append(min(100.0, max(0.0, (fdi / 20.0) * 100.0)))
        if hitech is not None:
            # Hi-tech exports: 0-50% of manufactured exports -> 0-100
            normed.append(min(100.0, (hitech / 50.0) * 100.0))

        openness_composite = float(np.mean(normed))
        score = max(0.0, 100.0 - openness_composite)

        return {
            "score": round(score, 1),
            "country": country,
            "fdi_pct_gdp": round(fdi, 4) if fdi is not None else None,
            "hitech_exports_pct": round(hitech, 2) if hitech is not None else None,
            "openness_composite": round(openness_composite, 2),
            "interpretation": "High score = closed innovation system with limited external knowledge flows",
        }
