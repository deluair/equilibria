"""Innovation Efficiency module.

Measures how efficiently a country converts R&D investment into
technology-intensive output:
- High-technology exports as % of manufactured exports (TX.VAL.TECH.MF.ZS)
- R&D expenditure as % of GDP (GB.XPD.RSDV.GD.ZS)

Score = max(0, rnd_pct - hitech_pct) * 10

When R&D spending is high but high-tech exports are low, the gap signals
an innovation bottleneck: resources are being invested but not translating
into internationally competitive technology products.

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class InnovationEfficiency(LayerBase):
    layer_id = "lNV"
    name = "Innovation Efficiency"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

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

        rnd_rows = await db.fetch_all(
            """
            SELECT dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GB.XPD.RSDV.GD.ZS'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        hitech: float | None = None
        rnd: float | None = None

        if hitech_rows:
            vals = [float(r["value"]) for r in hitech_rows if r["value"] is not None]
            hitech = float(np.mean(vals)) if vals else None

        if rnd_rows:
            vals = [float(r["value"]) for r in rnd_rows if r["value"] is not None]
            rnd = float(np.mean(vals)) if vals else None

        if hitech is None or rnd is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        # Positive gap (R&D > hi-tech exports) signals bottleneck
        gap = rnd - hitech
        score = min(100.0, max(0.0, gap * 10.0))

        return {
            "score": round(score, 1),
            "country": country,
            "rnd_pct_gdp": round(rnd, 4),
            "hitech_exports_pct": round(hitech, 2),
            "efficiency_gap": round(gap, 4),
            "interpretation": (
                "Positive gap (R&D > hi-tech exports) = innovation bottleneck; "
                "negative gap = strong tech commercialization"
            ),
        }
