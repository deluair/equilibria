"""National Innovation System (NIS) composite module.

Computes a composite score for national innovation capacity based on:
- R&D expenditure as % of GDP (GB.XPD.RSDV.GD.ZS)
- Gross tertiary school enrollment rate (SE.TER.ENRR)
- High-technology exports as % of manufactured exports (TX.VAL.TECH.MF.ZS)

A low composite score indicates a weak national innovation system with
limited research capacity, insufficient human capital formation, and
low technology content in exports.

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class NationalInnovationSystem(LayerBase):
    layer_id = "lNV"
    name = "National Innovation System"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        series_ids = [
            "GB.XPD.RSDV.GD.ZS",
            "SE.TER.ENRR",
            "TX.VAL.TECH.MF.ZS",
        ]

        values: dict[str, float | None] = {}
        for sid in series_ids:
            rows = await db.fetch_all(
                """
                SELECT dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                  AND dp.value IS NOT NULL
                ORDER BY dp.date DESC
                LIMIT 5
                """,
                (country, sid),
            )
            if rows:
                vals = [float(r["value"]) for r in rows if r["value"] is not None]
                values[sid] = float(np.mean(vals)) if vals else None
            else:
                values[sid] = None

        rnd = values["GB.XPD.RSDV.GD.ZS"]
        tertiary = values["SE.TER.ENRR"]
        hitech = values["TX.VAL.TECH.MF.ZS"]

        available = [v for v in [rnd, tertiary, hitech] if v is not None]
        if len(available) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        # Normalize each component to 0-100 using typical global ranges
        # R&D: 0-5% GDP -> 0-100
        rnd_norm = min(100.0, (rnd / 5.0) * 100.0) if rnd is not None else None
        # Tertiary enrollment: 0-100% -> 0-100
        tertiary_norm = min(100.0, max(0.0, tertiary)) if tertiary is not None else None
        # Hi-tech exports: 0-50% of manufactured exports -> 0-100
        hitech_norm = min(100.0, (hitech / 50.0) * 100.0) if hitech is not None else None

        normed = [v for v in [rnd_norm, tertiary_norm, hitech_norm] if v is not None]
        composite = float(np.mean(normed))

        # Low composite = weak NIS; invert so higher score = more stress
        score = max(0.0, 100.0 - composite)

        return {
            "score": round(score, 1),
            "country": country,
            "composite": round(composite, 2),
            "components": {
                "rnd_pct_gdp": round(rnd, 4) if rnd is not None else None,
                "tertiary_enrollment": round(tertiary, 2) if tertiary is not None else None,
                "hitech_exports_pct": round(hitech, 2) if hitech is not None else None,
            },
            "normalized": {
                "rnd": round(rnd_norm, 2) if rnd_norm is not None else None,
                "tertiary": round(tertiary_norm, 2) if tertiary_norm is not None else None,
                "hitech": round(hitech_norm, 2) if hitech_norm is not None else None,
            },
            "interpretation": "Low composite indicates weak national innovation system",
        }
