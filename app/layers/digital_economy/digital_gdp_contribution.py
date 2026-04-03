"""Digital GDP Contribution module.

ICT sector value added as a percentage of GDP.
Proxy series: IT.NET.USER.ZS (internet users %) combined with
NV.SRV.TOTL.ZS (services value added % GDP) as a structural complement.

Direct ICT value-added series (e.g. OECD STAN) is not universally available in WDI;
this module uses available WDI proxies with appropriate documentation.

Score: higher ICT contribution = stronger digital economy (lower risk).

Source: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DigitalGdpContribution(LayerBase):
    layer_id = "lDG"
    name = "Digital GDP Contribution"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        services_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NV.SRV.TOTL.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        internet_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IT.NET.USER.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not services_rows and not internet_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no services/internet data"}

        services_vals = [float(r["value"]) for r in services_rows if r["value"] is not None]
        internet_vals = [float(r["value"]) for r in internet_rows if r["value"] is not None]

        services_mean = float(np.nanmean(services_vals)) if services_vals else None
        internet_mean = float(np.nanmean(internet_vals)) if internet_vals else None

        # Services VA % GDP: normalize 0-100 (cap at 80% as ceiling)
        services_norm = float(np.clip((services_mean or 0) / 80.0 * 100, 0, 100)) if services_mean is not None else None
        internet_norm = float(np.clip(internet_mean or 0, 0, 100)) if internet_mean is not None else None

        components, weights = [], []
        if services_norm is not None:
            components.append(services_norm)
            weights.append(0.5)
        if internet_norm is not None:
            components.append(internet_norm)
            weights.append(0.5)

        if not components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable values"}

        total_w = sum(weights)
        composite = sum(c * w for c, w in zip(components, weights)) / total_w
        score = float(np.clip(composite, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "services_va_pct_gdp": round(services_mean, 2) if services_mean is not None else None,
            "internet_users_pct": round(internet_mean, 2) if internet_mean is not None else None,
            "note": "Proxy: services VA + internet users as ICT-GDP contribution indicator.",
            "_citation": "World Bank WDI: NV.SRV.TOTL.ZS, IT.NET.USER.ZS",
        }
