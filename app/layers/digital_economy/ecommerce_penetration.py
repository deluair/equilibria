"""E-commerce Penetration module.

E-commerce sales as a percentage of total retail sales.
Proxy: IT.NET.BBND.P2 (fixed broadband subscriptions per 100) + IT.NET.USER.ZS (internet users %)
used as a demand-side proxy for e-commerce readiness where direct retail data is unavailable.

Score: higher penetration = lower risk score (more digital economy depth).

Source: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class EcommercePenetration(LayerBase):
    layer_id = "lDG"
    name = "E-commerce Penetration"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

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

        broadband_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IT.NET.BBND.P2'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not internet_rows and not broadband_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no internet/broadband data"}

        internet_vals = [float(r["value"]) for r in internet_rows if r["value"] is not None]
        broadband_vals = [float(r["value"]) for r in broadband_rows if r["value"] is not None]

        internet_mean = float(np.nanmean(internet_vals)) if internet_vals else None
        broadband_mean = float(np.nanmean(broadband_vals)) if broadband_vals else None

        components, weights = [], []
        if internet_mean is not None:
            components.append(float(np.clip(internet_mean, 0, 100)))
            weights.append(0.6)
        if broadband_mean is not None:
            components.append(float(np.clip(broadband_mean, 0, 100)))
            weights.append(0.4)

        if not components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable values"}

        total_w = sum(weights)
        penetration = sum(c * w for c, w in zip(components, weights)) / total_w
        score = float(np.clip(penetration, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "internet_users_pct": round(internet_mean, 2) if internet_mean is not None else None,
            "broadband_per_100": round(broadband_mean, 2) if broadband_mean is not None else None,
            "note": "Proxy index: internet users + broadband as e-commerce demand-side indicator.",
            "_citation": "World Bank WDI: IT.NET.USER.ZS, IT.NET.BBND.P2",
        }
