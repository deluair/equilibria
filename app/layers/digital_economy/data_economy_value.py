"""Data Economy Value module.

Data value creation proxy index.
Uses IP.PAT.RESD (patent applications by residents) and
TX.VAL.TECH.MF.ZS (high-technology exports % of manufactured exports) as
proxies for knowledge/data economy depth.

High patent activity + high-tech export share = stronger data economy.

Score: higher = stronger data economy (lower risk, more digital sophistication).

Source: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DataEconomyValue(LayerBase):
    layer_id = "lDG"
    name = "Data Economy Value"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        patent_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IP.PAT.RESD'
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

        if not patent_rows and not hitech_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no patent/hi-tech export data"}

        patent_vals = [float(r["value"]) for r in patent_rows if r["value"] is not None]
        hitech_vals = [float(r["value"]) for r in hitech_rows if r["value"] is not None]

        patent_mean = float(np.nanmean(patent_vals)) if patent_vals else None
        hitech_mean = float(np.nanmean(hitech_vals)) if hitech_vals else None

        # Patents: log-normalize (large economies file hundreds of thousands)
        if patent_mean is not None:
            patent_norm = float(np.clip(np.log1p(patent_mean) / np.log1p(500_000) * 100, 0, 100))
        else:
            patent_norm = None

        hitech_norm = float(np.clip(hitech_mean or 0, 0, 100)) if hitech_mean is not None else None

        components, weights = [], []
        if patent_norm is not None:
            components.append(patent_norm)
            weights.append(0.5)
        if hitech_norm is not None:
            components.append(hitech_norm)
            weights.append(0.5)

        if not components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable values"}

        total_w = sum(weights)
        composite = sum(c * w for c, w in zip(components, weights)) / total_w
        score = float(np.clip(composite, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "patent_applications_residents": round(patent_mean, 0) if patent_mean is not None else None,
            "hitech_exports_pct_manf": round(hitech_mean, 2) if hitech_mean is not None else None,
            "note": "Proxy: patents (log-normalized) + hi-tech export share as data economy indicator.",
            "_citation": "World Bank WDI: IP.PAT.RESD, TX.VAL.TECH.MF.ZS",
        }
