"""Cyber Economic Risk module.

Economic cost of cyber incidents as a percentage of GDP.
Proxy: IT.NET.SECR.P6 (secure internet servers per 1 million people) as a structural
indicator of cyber resilience; lower server density = higher cyber economic risk exposure.

Higher server density = stronger cyber infrastructure = lower risk.
Score inverted so higher score = greater cyber economic risk.

Source: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CyberEconomicRisk(LayerBase):
    layer_id = "lDG"
    name = "Cyber Economic Risk"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        server_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IT.NET.SECR.P6'
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

        if not server_rows and not internet_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no secure server/internet data"}

        server_vals = [float(r["value"]) for r in server_rows if r["value"] is not None]
        internet_vals = [float(r["value"]) for r in internet_rows if r["value"] is not None]

        server_mean = float(np.nanmean(server_vals)) if server_vals else None
        internet_mean = float(np.nanmean(internet_vals)) if internet_vals else None

        # Secure servers per 1M: log-normalize (frontier countries: ~100k+)
        if server_mean is not None:
            server_norm = float(np.clip(np.log1p(server_mean) / np.log1p(200_000) * 100, 0, 100))
        else:
            server_norm = None

        internet_norm = float(np.clip(internet_mean or 0, 0, 100)) if internet_mean is not None else None

        components, weights = [], []
        if server_norm is not None:
            components.append(server_norm)
            weights.append(0.6)
        if internet_norm is not None:
            components.append(internet_norm)
            weights.append(0.4)

        if not components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable values"}

        total_w = sum(weights)
        resilience = sum(c * w for c, w in zip(components, weights)) / total_w
        # Invert: high resilience = low cyber risk
        score = float(np.clip(100.0 - resilience, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "secure_servers_per_1m": round(server_mean, 1) if server_mean is not None else None,
            "internet_users_pct": round(internet_mean, 2) if internet_mean is not None else None,
            "resilience_index": round(resilience, 2),
            "note": "Higher score = greater cyber economic risk (inverted resilience index).",
            "_citation": "World Bank WDI: IT.NET.SECR.P6, IT.NET.USER.ZS",
        }
