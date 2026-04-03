"""SME Export Participation module.

Measures SME participation in international trade using:
- TX.VAL.MRCH.CD.WT: Merchandise exports (current USD) -- total export base
- NV.IND.MANF.ZS: Manufacturing value added (% GDP) -- SME-intensive sector proxy
- IC.BUS.NDNS.ZS: New business density -- entrepreneurial activity base

Direct SME export data is rarely available in open databases. This module uses
the share of manufacturing value added (a sector with high SME density) in GDP
alongside the entrepreneurial activity base as a structural proxy for SME export
participation. Low manufacturing share combined with low business formation
suggests limited SME integration into global value chains.

Score: higher score = lower SME export participation = more stress.

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SmeExportParticipation(LayerBase):
    layer_id = "lER"
    name = "SME Export Participation"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        manuf_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NV.IND.MANF.ZS'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        entry_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IC.BUS.NDNS.ZS'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        manuf_share: float | None = None
        entry_rate: float | None = None

        if manuf_rows:
            vals = [float(r["value"]) for r in manuf_rows if r["value"] is not None]
            manuf_share = float(np.mean(vals)) if vals else None

        if entry_rows:
            vals = [float(r["value"]) for r in entry_rows if r["value"] is not None]
            entry_rate = float(np.mean(vals)) if vals else None

        if manuf_share is None and entry_rate is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no manufacturing or business density data",
            }

        score_parts: list[float] = []

        if manuf_share is not None:
            # Manufacturing: 0-40% GDP. Higher = more SME export capacity. Clamp at 30%.
            norm = min(100.0, (manuf_share / 30.0) * 100.0)
            score_parts.append(max(0.0, 100.0 - norm))

        if entry_rate is not None:
            # Business density: 0-10 per 1,000. Higher = more SME participants.
            norm = min(100.0, (entry_rate / 10.0) * 100.0)
            score_parts.append(max(0.0, 100.0 - norm))

        score = float(np.mean(score_parts))

        return {
            "score": round(score, 1),
            "country": country,
            "manufacturing_pct_gdp": round(manuf_share, 2) if manuf_share is not None else None,
            "business_density_per_1000": round(entry_rate, 4) if entry_rate is not None else None,
            "proxy_note": "Direct SME export share unavailable; proxied via manufacturing VA + business density",
            "interpretation": "High score = low manufacturing + low density = weak SME export participation",
        }
