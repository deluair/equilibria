"""Digital Productivity Premium module.

TFP differential between digital and non-digital firms.
Proxy: GB.XPD.RSDV.GD.ZS (R&D expenditure % GDP) and
TX.VAL.TECH.MF.ZS (high-technology exports % manufactured exports).

Higher R&D spending + hi-tech export intensity = stronger digital productivity premium.

Score: higher = larger productivity premium in favor of digital firms.

Source: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DigitalProductivityPremium(LayerBase):
    layer_id = "lDG"
    name = "Digital Productivity Premium"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rnd_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GB.XPD.RSDV.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        hitech_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'TX.VAL.TECH.MF.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not rnd_rows and not hitech_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no R&D/hi-tech data"}

        rnd_vals = [float(r["value"]) for r in rnd_rows if r["value"] is not None]
        hitech_vals = [float(r["value"]) for r in hitech_rows if r["value"] is not None]

        rnd_mean = float(np.nanmean(rnd_vals)) if rnd_vals else None
        hitech_mean = float(np.nanmean(hitech_vals)) if hitech_vals else None

        # R&D % GDP: cap at 5% as ceiling for normalization
        rnd_norm = float(np.clip((rnd_mean or 0) / 5.0 * 100, 0, 100)) if rnd_mean is not None else None
        hitech_norm = float(np.clip(hitech_mean or 0, 0, 100)) if hitech_mean is not None else None

        components, weights = [], []
        if rnd_norm is not None:
            components.append(rnd_norm)
            weights.append(0.5)
        if hitech_norm is not None:
            components.append(hitech_norm)
            weights.append(0.5)

        if not components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable values"}

        total_w = sum(weights)
        premium = sum(c * w for c, w in zip(components, weights)) / total_w
        score = float(np.clip(premium, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "rnd_pct_gdp": round(rnd_mean, 2) if rnd_mean is not None else None,
            "hitech_exports_pct_manf": round(hitech_mean, 2) if hitech_mean is not None else None,
            "note": "Higher score = larger TFP premium in digitally intensive sectors.",
            "_citation": "World Bank WDI: GB.XPD.RSDV.GD.ZS, TX.VAL.TECH.MF.ZS",
        }
