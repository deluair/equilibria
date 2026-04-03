"""Internet Economy Size module.

Internet economy value as a share of GDP.
Proxies: IT.NET.USER.ZS (internet users % population) and
IT.NET.BBND.P2 (fixed broadband subscriptions per 100 people).

Combines connectivity indicators as a structural proxy for internet economy scale
where direct internet economy GDP estimates are not available in WDI.

Score: larger internet economy = lower risk (more developed digital sector).

Source: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class InternetEconomySize(LayerBase):
    layer_id = "lDG"
    name = "Internet Economy Size"

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

        mobile_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IT.CEL.SETS.P2'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not internet_rows and not mobile_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no connectivity data"}

        internet_vals = [float(r["value"]) for r in internet_rows if r["value"] is not None]
        mobile_vals = [float(r["value"]) for r in mobile_rows if r["value"] is not None]

        internet_mean = float(np.nanmean(internet_vals)) if internet_vals else None
        mobile_mean = float(np.nanmean(mobile_vals)) if mobile_vals else None

        # Mobile subscriptions can exceed 100 (multiple SIMs); cap at 150
        internet_norm = float(np.clip(internet_mean or 0, 0, 100)) if internet_mean is not None else None
        mobile_norm = float(np.clip((mobile_mean or 0) / 150.0 * 100, 0, 100)) if mobile_mean is not None else None

        components, weights = [], []
        if internet_norm is not None:
            components.append(internet_norm)
            weights.append(0.6)
        if mobile_norm is not None:
            components.append(mobile_norm)
            weights.append(0.4)

        if not components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable values"}

        total_w = sum(weights)
        composite = sum(c * w for c, w in zip(components, weights)) / total_w
        score = float(np.clip(composite, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "internet_users_pct": round(internet_mean, 2) if internet_mean is not None else None,
            "mobile_subs_per_100": round(mobile_mean, 2) if mobile_mean is not None else None,
            "note": "Connectivity proxy for internet economy scale. Score = penetration index (0-100).",
            "_citation": "World Bank WDI: IT.NET.USER.ZS, IT.CEL.SETS.P2",
        }
