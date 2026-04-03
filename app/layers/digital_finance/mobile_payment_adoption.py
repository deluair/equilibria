"""Mobile Payment Adoption module.

Mobile financial services readiness proxy using:
  - IT.CEL.SETS.P2: mobile cellular subscriptions per 100 people (WDI)
  - IT.NET.USER.ZS: internet users % population (WDI)

Low mobile penetration + low internet => low digital payment readiness => high score (stress).

Score = 100 - composite_readiness, where composite_readiness combines
mobile and internet penetration normalized to 0-100.

Source: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MobilePaymentAdoption(LayerBase):
    layer_id = "lDF"
    name = "Mobile Payment Adoption"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

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

        if not mobile_rows and not internet_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        mobile_vals = [float(r["value"]) for r in mobile_rows if r["value"] is not None]
        internet_vals = [float(r["value"]) for r in internet_rows if r["value"] is not None]

        mobile_mean = float(np.nanmean(mobile_vals)) if mobile_vals else None
        internet_mean = float(np.nanmean(internet_vals)) if internet_vals else None

        # Normalize mobile: cap at 150 subs/100 (many countries exceed 100)
        mobile_norm = float(np.clip((mobile_mean or 0) / 150.0 * 100, 0, 100)) if mobile_mean is not None else 50.0
        internet_norm = float(np.clip(internet_mean or 0, 0, 100)) if internet_mean is not None else 50.0

        # Equal weight composite readiness
        weights = []
        components = []
        if mobile_mean is not None:
            weights.append(0.4)
            components.append(mobile_norm)
        if internet_mean is not None:
            weights.append(0.6)
            components.append(internet_norm)

        total_w = sum(weights)
        composite_readiness = sum(c * w for c, w in zip(components, weights)) / total_w if total_w > 0 else 50.0
        score = float(np.clip(100.0 - composite_readiness, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "mobile_per_100": round(mobile_mean, 2) if mobile_mean is not None else None,
            "internet_users_pct": round(internet_mean, 2) if internet_mean is not None else None,
            "mobile_norm": round(mobile_norm, 2),
            "internet_norm": round(internet_norm, 2),
            "composite_readiness": round(composite_readiness, 2),
            "note": "Score 0 = high digital payment readiness. Score 100 = no readiness.",
            "_citation": "World Bank WDI: IT.CEL.SETS.P2, IT.NET.USER.ZS",
        }
