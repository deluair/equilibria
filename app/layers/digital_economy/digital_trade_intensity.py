"""Digital Trade Intensity module.

Digitally deliverable services as a percentage of total exports.
Proxy: BX.GSR.ROYL.CD (charges for use of intellectual property, receipts) +
BX.GSR.CMCP.ZS (communications/computer/information services % of commercial service exports).

Higher ICT services export share = greater digital trade intensity.

Score: higher = more digitally intensive trade (strength, not risk).

Source: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DigitalTradeIntensity(LayerBase):
    layer_id = "lDG"
    name = "Digital Trade Intensity"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        ict_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'BX.GSR.CMCP.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        services_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'BX.GSR.NFSV.CD'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not ict_rows and not services_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no ICT services export data"}

        ict_vals = [float(r["value"]) for r in ict_rows if r["value"] is not None]
        services_vals = [float(r["value"]) for r in services_rows if r["value"] is not None]

        ict_mean = float(np.nanmean(ict_vals)) if ict_vals else None
        services_mean = float(np.nanmean(services_vals)) if services_vals else None

        components, weights = [], []
        if ict_mean is not None:
            # BX.GSR.CMCP.ZS is already a percentage of commercial service exports
            components.append(float(np.clip(ict_mean, 0, 100)))
            weights.append(0.7)
        if services_mean is not None:
            # Log-normalize absolute services receipts
            svc_norm = float(np.clip(np.log1p(services_mean) / np.log1p(1e12) * 100, 0, 100))
            components.append(svc_norm)
            weights.append(0.3)

        if not components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable values"}

        total_w = sum(weights)
        intensity = sum(c * w for c, w in zip(components, weights)) / total_w
        score = float(np.clip(intensity, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "ict_services_pct_commercial_exports": round(ict_mean, 2) if ict_mean is not None else None,
            "services_receipts_usd": round(services_mean, 0) if services_mean is not None else None,
            "note": "Higher score = more digitally deliverable services in export mix.",
            "_citation": "World Bank WDI: BX.GSR.CMCP.ZS, BX.GSR.NFSV.CD",
        }
